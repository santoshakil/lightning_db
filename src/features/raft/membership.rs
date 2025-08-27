use serde::{Serialize, Deserialize};
use crate::core::error::Error;
use super::core::{NodeId, LogIndex};
use std::collections::{HashSet, HashMap};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterConfig {
    pub members: Vec<ClusterMember>,
    pub learners: Vec<ClusterMember>,
    pub version: u64,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            members: Vec::new(),
            learners: Vec::new(),
            version: 0,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterMember {
    pub id: NodeId,
    pub address: String,
    pub voting: bool,
    pub metadata: HashMap<String, String>,
}

impl ClusterMember {
    pub fn new(id: NodeId, address: String) -> Self {
        Self {
            id,
            address,
            voting: true,
            metadata: HashMap::new(),
        }
    }

    pub fn new_learner(id: NodeId, address: String) -> Self {
        Self {
            id,
            address,
            voting: false,
            metadata: HashMap::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MembershipChange {
    AddMember(ClusterMember),
    RemoveMember(NodeId),
    PromoteLearner(NodeId),
    DemoteToLearner(NodeId),
    ReplaceNode { old: NodeId, new: ClusterMember },
}

pub struct MembershipManager {
    current_config: parking_lot::RwLock<ClusterConfig>,
    pending_config: parking_lot::RwLock<Option<ClusterConfig>>,
    joint_consensus: parking_lot::RwLock<Option<JointConfig>>,
}

#[derive(Debug, Clone)]
struct JointConfig {
    old_config: ClusterConfig,
    new_config: ClusterConfig,
    start_index: LogIndex,
}

impl MembershipManager {
    pub fn new(initial_config: ClusterConfig) -> Self {
        Self {
            current_config: parking_lot::RwLock::new(initial_config),
            pending_config: parking_lot::RwLock::new(None),
            joint_consensus: parking_lot::RwLock::new(None),
        }
    }

    pub fn propose_change(&self, change: MembershipChange) -> Result<ClusterConfig, Error> {
        if self.pending_config.read().is_some() {
            return Err(Error::InvalidOperation {
                reason: "Configuration change already in progress".to_string(),
            });
        }

        let current = self.current_config.read().clone();
        let new_config = self.apply_change(current, change)?;
        
        self.validate_config(&new_config)?;
        
        *self.pending_config.write() = Some(new_config.clone());
        
        Ok(new_config)
    }

    fn apply_change(&self, mut config: ClusterConfig, change: MembershipChange) -> Result<ClusterConfig, Error> {
        match change {
            MembershipChange::AddMember(member) => {
                if self.member_exists(&config, member.id) {
                    return Err(Error::InvalidOperation {
                        reason: format!("Member {} already exists", member.id),
                    });
                }
                
                if member.voting {
                    config.members.push(member);
                } else {
                    config.learners.push(member);
                }
            },
            
            MembershipChange::RemoveMember(node_id) => {
                config.members.retain(|m| m.id != node_id);
                config.learners.retain(|m| m.id != node_id);
            },
            
            MembershipChange::PromoteLearner(node_id) => {
                if let Some(pos) = config.learners.iter().position(|m| m.id == node_id) {
                    let mut member = config.learners.remove(pos);
                    member.voting = true;
                    config.members.push(member);
                } else {
                    return Err(Error::NotFound(format!("Learner {} not found", node_id)));
                }
            },
            
            MembershipChange::DemoteToLearner(node_id) => {
                if let Some(pos) = config.members.iter().position(|m| m.id == node_id) {
                    let mut member = config.members.remove(pos);
                    member.voting = false;
                    config.learners.push(member);
                } else {
                    return Err(Error::NotFound(format!("Member {} not found", node_id)));
                }
            },
            
            MembershipChange::ReplaceNode { old, new } => {
                if let Some(pos) = config.members.iter().position(|m| m.id == old) {
                    config.members[pos] = new;
                } else {
                    return Err(Error::NotFound(format!("Member {} not found", old)));
                }
            },
        }
        
        config.version += 1;
        Ok(config)
    }

    fn member_exists(&self, config: &ClusterConfig, node_id: NodeId) -> bool {
        config.members.iter().any(|m| m.id == node_id) ||
        config.learners.iter().any(|m| m.id == node_id)
    }

    fn validate_config(&self, config: &ClusterConfig) -> Result<(), Error> {
        // Must have at least one voting member
        if config.members.is_empty() {
            return Err(Error::InvalidOperation {
                reason: "Cluster must have at least one voting member".to_string(),
            });
        }
        
        // Check for duplicate IDs
        let mut seen_ids = HashSet::new();
        for member in &config.members {
            if !seen_ids.insert(member.id) {
                return Err(Error::InvalidOperation {
                    reason: format!("Duplicate member ID: {}", member.id),
                });
            }
        }
        
        for learner in &config.learners {
            if !seen_ids.insert(learner.id) {
                return Err(Error::InvalidOperation {
                    reason: format!("Duplicate learner ID: {}", learner.id),
                });
            }
        }
        
        Ok(())
    }

    pub fn enter_joint_consensus(&self, new_config: ClusterConfig, start_index: LogIndex) {
        let current = self.current_config.read().clone();
        
        *self.joint_consensus.write() = Some(JointConfig {
            old_config: current,
            new_config,
            start_index,
        });
    }

    pub fn commit_configuration(&self, index: LogIndex) {
        if let Some(joint) = self.joint_consensus.read().as_ref() {
            if index >= joint.start_index {
                *self.current_config.write() = joint.new_config.clone();
                *self.joint_consensus.write() = None;
                *self.pending_config.write() = None;
            }
        }
    }

    pub fn abort_configuration_change(&self) {
        *self.pending_config.write() = None;
        *self.joint_consensus.write() = None;
    }

    pub fn is_member(&self, node_id: NodeId) -> bool {
        let config = self.current_config.read();
        config.members.iter().any(|m| m.id == node_id)
    }

    pub fn is_learner(&self, node_id: NodeId) -> bool {
        let config = self.current_config.read();
        config.learners.iter().any(|m| m.id == node_id)
    }

    pub fn get_voting_members(&self) -> Vec<NodeId> {
        let config = self.current_config.read();
        
        if let Some(joint) = self.joint_consensus.read().as_ref() {
            // In joint consensus, need majority from both old and new configs
            let mut members = HashSet::new();
            
            for member in &joint.old_config.members {
                if member.voting {
                    members.insert(member.id);
                }
            }
            
            for member in &joint.new_config.members {
                if member.voting {
                    members.insert(member.id);
                }
            }
            
            members.into_iter().collect()
        } else {
            config.members.iter()
                .filter(|m| m.voting)
                .map(|m| m.id)
                .collect()
        }
    }

    pub fn get_all_members(&self) -> Vec<NodeId> {
        let config = self.current_config.read();
        let mut members: Vec<_> = config.members.iter().map(|m| m.id).collect();
        members.extend(config.learners.iter().map(|m| m.id));
        members
    }

    pub fn get_member_address(&self, node_id: NodeId) -> Option<String> {
        let config = self.current_config.read();
        
        config.members.iter()
            .chain(config.learners.iter())
            .find(|m| m.id == node_id)
            .map(|m| m.address.clone())
    }

    pub fn requires_joint_consensus(&self, change: &MembershipChange) -> bool {
        match change {
            MembershipChange::AddMember(_) | 
            MembershipChange::RemoveMember(_) |
            MembershipChange::ReplaceNode { .. } => true,
            MembershipChange::PromoteLearner(_) |
            MembershipChange::DemoteToLearner(_) => false,
        }
    }

    pub fn is_configuration_change_safe(&self) -> bool {
        self.pending_config.read().is_none() && self.joint_consensus.read().is_none()
    }

    pub fn get_current_config(&self) -> ClusterConfig {
        self.current_config.read().clone()
    }

    pub fn get_pending_config(&self) -> Option<ClusterConfig> {
        self.pending_config.read().clone()
    }
}

pub fn calculate_quorum_size(voting_members: usize) -> usize {
    (voting_members / 2) + 1
}

pub fn is_quorum(votes: usize, total_members: usize) -> bool {
    votes >= calculate_quorum_size(total_members)
}