use super::*;
use std::sync::atomic::Ordering;
use std::cmp::min;

impl RaftNode {
    /// Replicate log entries to all followers
    pub async fn replicate_entries(&self) -> Result<()> {
        if self.state() != NodeState::Leader {
            return Ok(());
        }
        
        let peers: Vec<NodeId> = {
            let membership = self.membership.read();
            membership.current.keys()
                .filter(|id| **id != self.config.node_id)
                .cloned()
                .collect()
        };
        
        // Send AppendEntries to each peer
        let mut replication_futures = Vec::new();
        for peer_id in peers {
            let node = self.clone();
            replication_futures.push(tokio::spawn(async move {
                node.send_append_entries(peer_id, false).await
            }));
        }
        
        // Wait for responses
        for future in replication_futures {
            let _ = future.await;
        }
        
        // Update commit index based on replication
        self.update_commit_index().await?;
        
        Ok(())
    }
    
    /// Send AppendEntries RPC to a peer
    pub async fn send_append_entries(&self, peer_id: NodeId, is_heartbeat: bool) -> Result<()> {
        let (args, entries_to_send) = {
            let persistent = self.persistent.read();
            let leader_state = self.leader.read();
            
            if let Some(ref leader) = *leader_state {
                let next_index = *leader.next_index.get(&peer_id).unwrap_or(&1);
                let prev_log_index = if next_index > 0 { next_index - 1 } else { 0 };
                let prev_log_term = self.get_log_term(prev_log_index).unwrap_or(0);
                
                // Determine entries to send
                let entries = if is_heartbeat {
                    Vec::new()
                } else {
                    let mut entries_to_send = Vec::new();
                    let max_entries = self.config.max_append_entries;
                    
                    for entry in &persistent.log {
                        if entry.index >= next_index && entries_to_send.len() < max_entries {
                            entries_to_send.push(entry.clone());
                        }
                    }
                    
                    entries_to_send
                };
                
                let entries_count = entries.len();
                
                let args = AppendEntriesArgs {
                    term: persistent.current_term,
                    leader_id: self.config.node_id,
                    prev_log_index,
                    prev_log_term,
                    entries,
                    leader_commit: self.volatile.commit_index.load(Ordering::Acquire),
                    read_index: Some(leader.read_index),
                };
                
                (args, entries_count)
            } else {
                // Not leader anymore
                return Ok(());
            }
        };
        
        // Record in-flight request if not heartbeat
        if !is_heartbeat && entries_to_send > 0 {
            let mut leader_state = self.leader.write();
            if let Some(ref mut leader) = *leader_state {
                leader.in_flight.insert(peer_id, InFlightRequest {
                    prev_log_index: args.prev_log_index,
                    entries: args.entries.clone(),
                    sent_at: Instant::now(),
                });
            }
        }
        
        // Send RPC
        match self.rpc.send_append_entries(peer_id, args.clone()).await {
            Ok(reply) => {
                self.handle_append_entries_response(peer_id, args, reply).await?;
            }
            Err(e) => {
                // Log error but don't fail
                eprintln!("Failed to send AppendEntries to {}: {}", peer_id, e);
            }
        }
        
        Ok(())
    }
    
    /// Handle AppendEntries response
    async fn handle_append_entries_response(
        &self,
        peer_id: NodeId,
        args: AppendEntriesArgs,
        reply: AppendEntriesReply,
    ) -> Result<()> {
        // Check if we're still leader
        if self.state() != NodeState::Leader {
            return Ok(());
        }
        
        // Check term
        if reply.term > self.current_term() {
            self.become_follower(reply.term).await?;
            return Ok(());
        }
        
        let mut leader_state = self.leader.write();
        if let Some(ref mut leader) = *leader_state {
            // Remove from in-flight
            leader.in_flight.remove(&peer_id);
            
            if reply.success {
                // Update match_index and next_index
                let new_match_index = args.prev_log_index + args.entries.len() as u64;
                leader.match_index.insert(peer_id, new_match_index);
                leader.next_index.insert(peer_id, new_match_index + 1);
                
                // Update metrics
                self.metrics.entries_replicated.fetch_add(args.entries.len() as u64, Ordering::Relaxed);
            } else {
                // Replication failed, decrement next_index
                if let Some(next_index) = leader.next_index.get_mut(&peer_id) {
                    // Use conflict information for faster catch-up
                    if let (Some(conflict_term), Some(conflict_index)) = (reply.conflict_term, reply.conflict_index) {
                        // Find the first entry with conflict_term
                        let persistent = self.persistent.read();
                        let mut found = false;
                        
                        for entry in persistent.log.iter().rev() {
                            if entry.term == conflict_term {
                                *next_index = entry.index + 1;
                                found = true;
                                break;
                            }
                        }
                        
                        if !found {
                            *next_index = conflict_index;
                        }
                    } else {
                        // Simple back-off
                        *next_index = (*next_index).saturating_sub(1).max(1);
                    }
                }
            }
        }
        
        Ok(())
    }
    
    /// Handle incoming AppendEntries RPC
    pub async fn handle_append_entries(&self, args: AppendEntriesArgs) -> Result<AppendEntriesReply> {
        let mut persistent = self.persistent.write();
        
        // Reply false if term < currentTerm
        if args.term < persistent.current_term {
            return Ok(AppendEntriesReply {
                term: persistent.current_term,
                success: false,
                last_log_index: persistent.log.last().map(|e| e.index).unwrap_or(0),
                conflict_term: None,
                conflict_index: None,
            });
        }
        
        // Update term and become follower if necessary
        if args.term > persistent.current_term {
            persistent.current_term = args.term;
            persistent.voted_for = None;
            
            // Persist state
            self.storage.save_state(&persistent)?;
            
            // Become follower
            self.volatile.state.store(NodeState::Follower as u8, Ordering::Release);
            *self.leader.write() = None;
        }
        
        // Reset election timer
        drop(persistent); // Release lock before async call
        self.reset_election_timer().await;
        
        // Update leader info
        self.metrics.current_leader.store(args.leader_id, Ordering::Release);
        self.metrics.last_leader_contact.store(
            SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs(),
            Ordering::Release
        );
        
        let mut persistent = self.persistent.write();
        
        // Check if we have the previous log entry
        if args.prev_log_index > 0 {
            let has_prev_entry = persistent.log.iter()
                .any(|e| e.index == args.prev_log_index && e.term == args.prev_log_term);
            
            if !has_prev_entry {
                // Find conflict information
                let (conflict_term, conflict_index) = if let Some(entry) = persistent.log.iter()
                    .find(|e| e.index == args.prev_log_index) {
                    // Have entry at prev_log_index but wrong term
                    let term = entry.term;
                    let first_index = persistent.log.iter()
                        .find(|e| e.term == term)
                        .map(|e| e.index)
                        .unwrap_or(1);
                    (Some(term), Some(first_index))
                } else {
                    // Don't have entry at prev_log_index
                    (None, Some(persistent.log.last().map(|e| e.index + 1).unwrap_or(1)))
                };
                
                return Ok(AppendEntriesReply {
                    term: persistent.current_term,
                    success: false,
                    last_log_index: persistent.log.last().map(|e| e.index).unwrap_or(0),
                    conflict_term,
                    conflict_index,
                });
            }
        }
        
        // Delete conflicting entries and append new ones
        if !args.entries.is_empty() {
            let mut delete_from = None;
            
            // Find first conflicting entry
            for new_entry in &args.entries {
                if let Some(existing) = persistent.log.iter()
                    .find(|e| e.index == new_entry.index) {
                    if existing.term != new_entry.term {
                        delete_from = Some(new_entry.index);
                        break;
                    }
                } else {
                    // Entry doesn't exist, append from here
                    break;
                }
            }
            
            // Delete conflicting entries
            if let Some(index) = delete_from {
                persistent.log.retain(|e| e.index < index);
                self.storage.delete_entries_from(index)?;
            }
            
            // Append new entries
            let mut new_entries = Vec::new();
            for entry in &args.entries {
                if persistent.log.iter().all(|e| e.index != entry.index) {
                    persistent.log.push(entry.clone());
                    new_entries.push(entry.clone());
                }
            }
            
            if !new_entries.is_empty() {
                self.storage.append_entries(&new_entries)?;
            }
        }
        
        // Update commit index
        if args.leader_commit > self.volatile.commit_index.load(Ordering::Acquire) {
            let last_log_index = persistent.log.last().map(|e| e.index).unwrap_or(0);
            let new_commit_index = min(args.leader_commit, last_log_index);
            self.volatile.commit_index.store(new_commit_index, Ordering::Release);
            
            // Apply committed entries
            drop(persistent); // Release lock before applying
            self.apply_committed_entries().await?;
        }
        
        Ok(AppendEntriesReply {
            term: self.current_term(),
            success: true,
            last_log_index: self.persistent.read().log.last().map(|e| e.index).unwrap_or(0),
            conflict_term: None,
            conflict_index: None,
        })
    }
    
    /// Update commit index based on replication
    async fn update_commit_index(&self) -> Result<()> {
        if self.state() != NodeState::Leader {
            return Ok(());
        }
        
        let leader_state = self.leader.read();
        if let Some(ref leader) = *leader_state {
            let persistent = self.persistent.read();
            let current_term = persistent.current_term;
            
            // Find highest index replicated on majority
            let mut match_indices: Vec<LogIndex> = leader.match_index.values().cloned().collect();
            match_indices.push(persistent.log.last().map(|e| e.index).unwrap_or(0)); // Include self
            match_indices.sort_unstable();
            
            let majority_index = match_indices.len() / 2;
            let new_commit_index = match_indices[majority_index];
            
            // Only commit entries from current term
            if new_commit_index > self.volatile.commit_index.load(Ordering::Acquire) {
                if let Some(entry) = persistent.log.iter().find(|e| e.index == new_commit_index) {
                    if entry.term == current_term {
                        self.volatile.commit_index.store(new_commit_index, Ordering::Release);
                        
                        // Apply committed entries
                        drop(persistent);
                        drop(leader_state);
                        self.apply_committed_entries().await?;
                        
                        // Notify pending requests
                        self.notify_pending_requests(new_commit_index).await?;
                    }
                }
            }
        }
        
        Ok(())
    }
    
    /// Apply committed entries to state machine
    async fn apply_committed_entries(&self) -> Result<()> {
        loop {
            let last_applied = self.volatile.last_applied.load(Ordering::Acquire);
            let commit_index = self.volatile.commit_index.load(Ordering::Acquire);
            
            if last_applied >= commit_index {
                break;
            }
            
            let next_index = last_applied + 1;
            
            // Get entry to apply
            let entry = {
                let persistent = self.persistent.read();
                persistent.log.iter()
                    .find(|e| e.index == next_index)
                    .cloned()
            };
            
            if let Some(entry) = entry {
                // Apply to state machine
                match self.state_machine.apply(&entry.command) {
                    Ok(_) => {
                        self.volatile.last_applied.store(next_index, Ordering::Release);
                    }
                    Err(e) => {
                        eprintln!("Failed to apply entry {}: {}", next_index, e);
                        // Continue applying other entries
                    }
                }
            } else {
                // Entry not found, shouldn't happen
                eprintln!("Entry {} not found in log", next_index);
                break;
            }
        }
        
        Ok(())
    }
    
    /// Notify pending client requests
    async fn notify_pending_requests(&self, commit_index: LogIndex) -> Result<()> {
        let mut leader_state = self.leader.write();
        if let Some(ref mut leader) = *leader_state {
            let mut completed = Vec::new();
            
            for (request_id, request) in &leader.pending_requests {
                // Check if request is committed
                let persistent = self.persistent.read();
                let is_committed = persistent.log.iter()
                    .any(|e| e.index <= commit_index && 
                         e.client_id == Some(*request_id) &&
                         matches!(&e.command, cmd if std::mem::discriminant(cmd) == std::mem::discriminant(&request.command)));
                
                if is_committed {
                    completed.push(*request_id);
                }
            }
            
            // Send responses
            for request_id in completed {
                if let Some(request) = leader.pending_requests.remove(&request_id) {
                    // Apply command and send response
                    match self.state_machine.apply(&request.command) {
                        Ok(result) => {
                            let _ = request.response_tx.send(Ok(result));
                        }
                        Err(e) => {
                            let _ = request.response_tx.send(Err(e));
                        }
                    }
                }
            }
        }
        
        Ok(())
    }
    
    /// Process client command
    pub async fn process_command(&self, command: Command, client_id: u64, request_id: u64) -> Result<Vec<u8>> {
        // Check if we're the leader
        if self.state() != NodeState::Leader {
            return Err(Error::NotLeader(self.current_leader()));
        }
        
        // Create log entry
        let entry = {
            let persistent = self.persistent.read();
            let index = persistent.log.last().map(|e| e.index + 1).unwrap_or(1);
            
            LogEntry {
                term: persistent.current_term,
                index,
                command: command.clone(),
                client_id: Some(client_id),
                request_id: Some(request_id),
            }
        };
        
        // Create response channel
        let (tx, rx) = oneshot::channel();
        
        // Add to pending requests
        {
            let mut leader_state = self.leader.write();
            if let Some(ref mut leader) = *leader_state {
                leader.pending_requests.insert(request_id, ClientRequest {
                    command,
                    response_tx: tx,
                    submitted_at: Instant::now(),
                });
            }
        }
        
        // Append to log
        {
            let mut persistent = self.persistent.write();
            persistent.log.push(entry.clone());
            self.storage.append_entries(&[entry])?;
        }
        
        // Replicate to followers
        self.replicate_entries().await?;
        
        // Wait for response
        match rx.await {
            Ok(result) => result,
            Err(_) => Err(Error::Internal("Request cancelled".to_string())),
        }
    }
}

// RPC methods are implemented in rpc.rs

#[cfg(test)]
mod tests {
    use super::*;
    
    #[tokio::test]
    async fn test_log_replication() {
        // TODO: Implement comprehensive replication tests
    }
    
    #[test]
    fn test_commit_index_calculation() {
        // TODO: Test commit index updates
    }
}