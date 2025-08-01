use super::*;
use std::sync::atomic::Ordering;
use rand::{thread_rng, Rng};
use tokio::time::Duration;

impl RaftNode {
    /// Start an election
    pub async fn start_election(&self, pre_vote: bool) -> Result<()> {
        let (mut current_term, last_log_index, last_log_term) = {
            let persistent = self.persistent.read().unwrap();
            let current_term = if pre_vote {
                persistent.current_term
            } else {
                persistent.current_term + 1
            };
            
            let (last_log_index, last_log_term) = if let Some(entry) = persistent.log.last() {
                (entry.index, entry.term)
            } else {
                (0, 0)
            };
            
            (current_term, last_log_index, last_log_term)
        };
        
        // For actual election, update state
        if !pre_vote {
            let mut persistent = self.persistent.write().unwrap();
            persistent.current_term = current_term;
            persistent.voted_for = Some(self.config.node_id);
            
            // Persist state
            self.storage.save_state(&persistent)?;
            
            // Become candidate
            self.volatile.state.store(NodeState::Candidate as u8, Ordering::Release);
            
            // Clear leader state
            *self.leader.write().unwrap() = None;
            
            // Update metrics
            self.metrics.elections.fetch_add(1, Ordering::Relaxed);
        }
        
        // Reset election timer
        self.reset_election_timer().await;
        
        // Send RequestVote to all peers
        let args = RequestVoteArgs {
            term: current_term,
            candidate_id: self.config.node_id,
            last_log_index,
            last_log_term,
            pre_vote,
        };
        
        // Get list of voting peers
        let voting_peers: Vec<NodeId> = {
            let membership = self.membership.read().unwrap();
            membership.current.iter()
                .filter(|(id, info)| **id != self.config.node_id && info.voting)
                .map(|(id, _)| *id)
                .collect()
        };
        
        // Need majority of votes (including self)
        let majority = (voting_peers.len() + 1) / 2 + 1;
        let mut votes = 1; // Vote for self
        let mut responses = 0;
        
        // Send vote requests in parallel
        let mut vote_futures = Vec::new();
        for peer_id in voting_peers {
            let rpc = self.rpc.clone();
            let args = args.clone();
            
            vote_futures.push(tokio::spawn(async move {
                rpc.send_request_vote(peer_id, args).await
            }));
        }
        
        // Collect votes
        for future in vote_futures {
            match future.await {
                Ok(Ok(reply)) => {
                    responses += 1;
                    
                    // Check if we need to update our term
                    if reply.term > current_term {
                        self.become_follower(reply.term).await?;
                        return Ok(());
                    }
                    
                    if reply.vote_granted {
                        votes += 1;
                        
                        // Check if we have majority
                        if votes >= majority {
                            if pre_vote {
                                // Won pre-vote, start actual election
                                return self.start_election(false).await;
                            } else {
                                // Won election, become leader
                                return self.become_leader().await;
                            }
                        }
                    }
                }
                _ => {} // Ignore errors and timeouts
            }
            
            // Check if we've lost the election
            if responses - votes + 1 >= majority {
                // Can't win anymore
                break;
            }
        }
        
        // Election failed, remain candidate/follower
        Ok(())
    }
    
    /// Become the leader
    pub async fn become_leader(&self) -> Result<()> {
        println!("Node {} became leader for term {}", 
                 self.config.node_id, 
                 self.current_term());
        
        // Update state
        self.volatile.state.store(NodeState::Leader as u8, Ordering::Release);
        
        // Initialize leader state
        let last_log_index = {
            let persistent = self.persistent.read().unwrap();
            persistent.log.last().map(|e| e.index).unwrap_or(0)
        };
        
        let mut leader_state = LeaderState {
            next_index: HashMap::new(),
            match_index: HashMap::new(),
            pending_requests: HashMap::new(),
            in_flight: HashMap::new(),
            read_index: last_log_index,
            pending_reads: VecDeque::new(),
        };
        
        // Initialize next_index and match_index for all peers
        let peers: Vec<NodeId> = {
            let membership = self.membership.read().unwrap();
            membership.current.keys().filter(|id| **id != self.config.node_id).cloned().collect()
        };
        
        for peer_id in peers {
            leader_state.next_index.insert(peer_id, last_log_index + 1);
            leader_state.match_index.insert(peer_id, 0);
        }
        
        *self.leader.write().unwrap() = Some(leader_state);
        
        // Update metrics
        self.metrics.elections_won.fetch_add(1, Ordering::Relaxed);
        self.metrics.current_leader.store(self.config.node_id, Ordering::Release);
        
        // Append no-op entry to establish leadership
        self.append_no_op().await?;
        
        // Start heartbeat timer
        self.reset_heartbeat_timer().await;
        
        Ok(())
    }
    
    // become_follower is implemented in mod.rs
    
    /// Handle RequestVote RPC
    pub async fn handle_request_vote(&self, args: RequestVoteArgs) -> Result<RequestVoteReply> {
        let mut persistent = self.persistent.write().unwrap();
        
        // Reply false if term < currentTerm
        if args.term < persistent.current_term {
            return Ok(RequestVoteReply {
                term: persistent.current_term,
                vote_granted: false,
                reason: Some("Term is lower than current term".to_string()),
            });
        }
        
        // Update term if necessary
        if args.term > persistent.current_term {
            persistent.current_term = args.term;
            persistent.voted_for = None;
            
            // Become follower
            self.volatile.state.store(NodeState::Follower as u8, Ordering::Release);
            *self.leader.write().unwrap() = None;
        }
        
        // Check if we can grant vote
        let vote_granted = if let Some(voted_for) = persistent.voted_for {
            // Already voted in this term
            voted_for == args.candidate_id
        } else {
            // Haven't voted yet, check log
            let (last_log_index, last_log_term) = if let Some(entry) = persistent.log.last() {
                (entry.index, entry.term)
            } else {
                (0, 0)
            };
            
            // Grant vote if candidate's log is at least as up-to-date as ours
            args.last_log_term > last_log_term || 
            (args.last_log_term == last_log_term && args.last_log_index >= last_log_index)
        };
        
        if vote_granted && !args.pre_vote {
            persistent.voted_for = Some(args.candidate_id);
            
            // Persist state
            self.storage.save_state(&persistent)?;
            
            // Reset election timer
            drop(persistent); // Release lock before async call
            self.reset_election_timer().await;
        }
        
        Ok(RequestVoteReply {
            term: if args.pre_vote { 
                self.persistent.read().unwrap().current_term 
            } else { 
                args.term 
            },
            vote_granted,
            reason: if !vote_granted {
                Some("Log is not up-to-date".to_string())
            } else {
                None
            },
        })
    }
    
    /// Append a no-op entry to establish leadership
    async fn append_no_op(&self) -> Result<()> {
        let entry = {
            let persistent = self.persistent.read().unwrap();
            let index = persistent.log.last().map(|e| e.index + 1).unwrap_or(1);
            
            LogEntry {
                term: persistent.current_term,
                index,
                command: Command::NoOp,
                client_id: None,
                request_id: None,
            }
        };
        
        // Append to our log
        {
            let mut persistent = self.persistent.write().unwrap();
            persistent.log.push(entry.clone());
            
            // Persist
            self.storage.append_entries(&[entry])?;
        }
        
        // Replicate to followers
        self.replicate_entries().await?;
        
        Ok(())
    }
    
    /// Send heartbeats to all followers
    pub async fn send_heartbeats(&self) -> Result<()> {
        if self.state() != NodeState::Leader {
            return Ok(());
        }
        
        let peers: Vec<NodeId> = {
            let membership = self.membership.read().unwrap();
            membership.current.keys().filter(|id| **id != self.config.node_id).cloned().collect()
        };
        
        // Send heartbeats in parallel
        let mut heartbeat_futures = Vec::new();
        for peer_id in peers {
            let node = self.clone();
            heartbeat_futures.push(tokio::spawn(async move {
                node.send_append_entries(peer_id, true).await
            }));
        }
        
        // Wait for all heartbeats to complete
        for future in heartbeat_futures {
            let _ = future.await;
        }
        
        Ok(())
    }
    
    /// Reset heartbeat timer
    pub async fn reset_heartbeat_timer(&self) {
        let timeout = Duration::from_millis(self.config.heartbeat_interval);
        let timer = tokio::time::sleep(timeout);
        *self.heartbeat_timer.lock() = Some(Box::pin(timer));
    }
    
    /// Check if a node's log is at least as up-to-date as ours
    fn is_log_up_to_date(&self, last_log_index: LogIndex, last_log_term: Term) -> bool {
        let persistent = self.persistent.read().unwrap();
        
        if let Some(entry) = persistent.log.last() {
            last_log_term > entry.term || 
            (last_log_term == entry.term && last_log_index >= entry.index)
        } else {
            // Empty log, candidate is up-to-date
            true
        }
    }
    
    /// Get the term of a log entry at the given index
    pub fn get_log_term(&self, index: LogIndex) -> Option<Term> {
        if index == 0 {
            return Some(0);
        }
        
        let persistent = self.persistent.read().unwrap();
        persistent.log.iter()
            .find(|entry| entry.index == index)
            .map(|entry| entry.term)
    }
}

// RPC methods are implemented in rpc.rs

#[cfg(test)]
mod tests {
    use super::*;
    
    #[tokio::test]
    async fn test_election_state_transitions() {
        // TODO: Implement comprehensive election tests
    }
    
    #[test]
    fn test_log_up_to_date_check() {
        // TODO: Test log comparison logic
    }
}