use crate::Result;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::RwLock;
use uuid::Uuid;
use async_trait::async_trait;
use std::fmt;
use chrono::{DateTime, Utc, Duration};
use regex::Regex;
use lazy_static::lazy_static;

#[derive(Debug, Clone, Serialize, Deserialize, Hash, Eq, PartialEq)]
pub struct Permission {
    id: Uuid,
    name: String,
    resource: String,
    action: Action,
    conditions: Vec<Condition>,
    effect: Effect,
    priority: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize, Hash, Eq, PartialEq)]
pub enum Action {
    Read,
    Write,
    Delete,
    Execute,
    Create,
    Update,
    List,
    Admin,
    Custom(String),
}

#[derive(Debug, Clone, Serialize, Deserialize, Hash, Eq, PartialEq)]
pub enum Effect {
    Allow,
    Deny,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Role {
    id: Uuid,
    name: String,
    description: String,
    permissions: HashSet<Uuid>,
    parent_roles: HashSet<Uuid>,
    metadata: HashMap<String, String>,
    created_at: DateTime<Utc>,
    modified_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Policy {
    id: Uuid,
    name: String,
    description: String,
    rules: Vec<PolicyRule>,
    priority: i32,
    enabled: bool,
    created_at: DateTime<Utc>,
    expires_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolicyRule {
    id: Uuid,
    resource_pattern: String,
    action_pattern: String,
    effect: Effect,
    conditions: Vec<Condition>,
    priority: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize, Hash, Eq, PartialEq)]
pub enum Condition {
    TimeRange { start: DateTime<Utc>, end: DateTime<Utc> },
    IpRange { cidrs: Vec<String> },
    AttributeMatch { key: String, value: String },
    AttributePattern { key: String, pattern: String },
    MFA { required: bool },
    ResourceOwner,
    DelegatedBy { principal: String },
    Custom { evaluator: String },
}

#[derive(Debug, Clone)]
pub struct AuthorizationContext {
    principal: Principal,
    resource: String,
    action: Action,
    attributes: HashMap<String, String>,
    request_time: DateTime<Utc>,
    ip_address: Option<String>,
    mfa_verified: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Principal {
    id: Uuid,
    principal_type: PrincipalType,
    roles: HashSet<Uuid>,
    permissions: HashSet<Uuid>,
    attributes: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum PrincipalType {
    User,
    Service,
    Group,
    Anonymous,
}

pub struct AuthorizationEngine {
    role_store: Arc<RoleStore>,
    policy_store: Arc<PolicyStore>,
    permission_store: Arc<PermissionStore>,
    evaluator: Arc<PolicyEvaluator>,
    cache: Arc<AuthorizationCache>,
    audit: Arc<AuthorizationAudit>,
    metrics: Arc<AuthorizationMetrics>,
}

struct RoleStore {
    roles: Arc<DashMap<Uuid, Role>>,
    role_hierarchy: Arc<RwLock<HashMap<Uuid, HashSet<Uuid>>>>,
    principal_roles: Arc<DashMap<Uuid, HashSet<Uuid>>>,
}

struct PolicyStore {
    policies: Arc<DashMap<Uuid, Policy>>,
    resource_policies: Arc<DashMap<String, HashSet<Uuid>>>,
    active_policies: Arc<RwLock<Vec<Policy>>>,
}

struct PermissionStore {
    permissions: Arc<DashMap<Uuid, Permission>>,
    resource_permissions: Arc<DashMap<String, HashSet<Uuid>>>,
}

struct PolicyEvaluator {
    condition_evaluators: Arc<DashMap<String, Arc<dyn ConditionEvaluator>>>,
    resource_matcher: Arc<ResourceMatcher>,
    action_matcher: Arc<ActionMatcher>,
}

struct AuthorizationCache {
    decisions: Arc<DashMap<String, CachedDecision>>,
    ttl: Duration,
}

#[derive(Debug, Clone)]
struct CachedDecision {
    result: bool,
    reason: String,
    timestamp: DateTime<Utc>,
    ttl: Duration,
}

struct AuthorizationAudit {
    logger: Arc<dyn AuditLogger>,
    enabled: bool,
    sensitive_resources: HashSet<String>,
}

#[async_trait]
trait AuditLogger: Send + Sync {
    async fn log_authorization(&self, event: AuthorizationEvent) -> Result<()>;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AuthorizationEvent {
    id: Uuid,
    timestamp: DateTime<Utc>,
    principal: String,
    resource: String,
    action: String,
    decision: bool,
    reason: String,
    policies_evaluated: Vec<Uuid>,
    duration_ms: u64,
}

struct AuthorizationMetrics {
    decisions_total: Arc<RwLock<u64>>,
    decisions_allowed: Arc<RwLock<u64>>,
    decisions_denied: Arc<RwLock<u64>>,
    cache_hits: Arc<RwLock<u64>>,
    cache_misses: Arc<RwLock<u64>>,
    evaluation_times: Arc<RwLock<Vec<u64>>>,
}

#[async_trait]
trait ConditionEvaluator: Send + Sync {
    async fn evaluate(&self, condition: &Condition, context: &AuthorizationContext) -> Result<bool>;
}

struct ResourceMatcher {
    patterns: Arc<RwLock<HashMap<String, Regex>>>,
}

struct ActionMatcher {
    action_hierarchy: Arc<RwLock<HashMap<Action, HashSet<Action>>>>,
}

impl AuthorizationEngine {
    pub async fn new(config: AuthorizationConfig) -> Result<Self> {
        Ok(Self {
            role_store: Arc::new(RoleStore::new()),
            policy_store: Arc::new(PolicyStore::new()),
            permission_store: Arc::new(PermissionStore::new()),
            evaluator: Arc::new(PolicyEvaluator::new()),
            cache: Arc::new(AuthorizationCache::new(config.cache_ttl)),
            audit: Arc::new(AuthorizationAudit::new(config.audit_config)),
            metrics: Arc::new(AuthorizationMetrics::new()),
        })
    }

    pub async fn authorize(&self, context: AuthorizationContext) -> Result<bool> {
        let start = std::time::Instant::now();
        
        let cache_key = self.generate_cache_key(&context);
        if let Some(cached) = self.cache.get(&cache_key).await? {
            self.metrics.record_cache_hit().await;
            return Ok(cached.result);
        }
        
        self.metrics.record_cache_miss().await;
        
        let principal_permissions = self.resolve_permissions(&context.principal).await?;
        let applicable_policies = self.get_applicable_policies(&context).await?;
        
        let mut decision = false;
        let mut reason = String::new();
        let mut policies_evaluated = Vec::new();
        
        for policy in applicable_policies {
            if !policy.enabled {
                continue;
            }
            
            if let Some(expiry) = policy.expires_at {
                if expiry < Utc::now() {
                    continue;
                }
            }
            
            policies_evaluated.push(policy.id);
            
            for rule in &policy.rules {
                if self.matches_rule(&rule, &context).await? {
                    let conditions_met = self.evaluate_conditions(&rule.conditions, &context).await?;
                    
                    if conditions_met {
                        match rule.effect {
                            Effect::Deny => {
                                decision = false;
                                reason = format!("Denied by policy {} rule {}", policy.name, rule.id);
                                break;
                            }
                            Effect::Allow => {
                                if !decision {
                                    decision = true;
                                    reason = format!("Allowed by policy {} rule {}", policy.name, rule.id);
                                }
                            }
                        }
                    }
                }
            }
            
            if !decision && matches!(rule.effect, Effect::Deny) {
                break;
            }
        }
        
        for perm_id in principal_permissions {
            if let Some(permission) = self.permission_store.permissions.get(&perm_id) {
                if self.matches_permission(&permission, &context).await? {
                    let conditions_met = self.evaluate_conditions(&permission.conditions, &context).await?;
                    
                    if conditions_met {
                        match permission.effect {
                            Effect::Deny => {
                                decision = false;
                                reason = format!("Denied by permission {}", permission.name);
                                break;
                            }
                            Effect::Allow => {
                                if !decision {
                                    decision = true;
                                    reason = format!("Allowed by permission {}", permission.name);
                                }
                            }
                        }
                    }
                }
            }
        }
        
        self.cache.put(cache_key, CachedDecision {
            result: decision,
            reason: reason.clone(),
            timestamp: Utc::now(),
            ttl: self.cache.ttl,
        }).await?;
        
        let duration_ms = start.elapsed().as_millis() as u64;
        self.metrics.record_decision(decision, duration_ms).await;
        
        if self.audit.enabled {
            self.audit.log_authorization(AuthorizationEvent {
                id: Uuid::new_v4(),
                timestamp: Utc::now(),
                principal: context.principal.id.to_string(),
                resource: context.resource.clone(),
                action: format!("{:?}", context.action),
                decision,
                reason,
                policies_evaluated,
                duration_ms,
            }).await?;
        }
        
        Ok(decision)
    }

    async fn resolve_permissions(&self, principal: &Principal) -> Result<HashSet<Uuid>> {
        let mut permissions = principal.permissions.clone();
        
        for role_id in &principal.roles {
            permissions.extend(self.get_role_permissions(role_id).await?);
        }
        
        Ok(permissions)
    }

    async fn get_role_permissions(&self, role_id: &Uuid) -> Result<HashSet<Uuid>> {
        let mut permissions = HashSet::new();
        
        if let Some(role) = self.role_store.roles.get(role_id) {
            permissions.extend(role.permissions.clone());
            
            for parent_id in &role.parent_roles {
                permissions.extend(Box::pin(self.get_role_permissions(parent_id)).await?);
            }
        }
        
        Ok(permissions)
    }

    async fn get_applicable_policies(&self, context: &AuthorizationContext) -> Result<Vec<Policy>> {
        let mut policies = Vec::new();
        
        let active_policies = self.policy_store.active_policies.read().await;
        for policy in active_policies.iter() {
            policies.push(policy.clone());
        }
        
        policies.sort_by_key(|p| -p.priority);
        Ok(policies)
    }

    async fn matches_rule(&self, rule: &PolicyRule, context: &AuthorizationContext) -> Result<bool> {
        let resource_match = self.evaluator.resource_matcher.matches(&rule.resource_pattern, &context.resource).await?;
        let action_match = self.evaluator.action_matcher.matches(&rule.action_pattern, &context.action).await?;
        Ok(resource_match && action_match)
    }

    async fn matches_permission(&self, permission: &Permission, context: &AuthorizationContext) -> Result<bool> {
        Ok(permission.resource == context.resource && permission.action == context.action)
    }

    async fn evaluate_conditions(&self, conditions: &[Condition], context: &AuthorizationContext) -> Result<bool> {
        for condition in conditions {
            if !self.evaluator.evaluate_condition(condition, context).await? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn generate_cache_key(&self, context: &AuthorizationContext) -> String {
        format!(
            "{}:{}:{:?}:{}",
            context.principal.id,
            context.resource,
            context.action,
            context.mfa_verified
        )
    }

    pub async fn create_role(&self, role: Role) -> Result<()> {
        self.role_store.roles.insert(role.id, role.clone());
        
        let mut hierarchy = self.role_store.role_hierarchy.write().await;
        for parent_id in &role.parent_roles {
            hierarchy.entry(*parent_id)
                .or_insert_with(HashSet::new)
                .insert(role.id);
        }
        
        Ok(())
    }

    pub async fn assign_role(&self, principal_id: Uuid, role_id: Uuid) -> Result<()> {
        self.role_store.principal_roles
            .entry(principal_id)
            .or_insert_with(HashSet::new)
            .insert(role_id);
        Ok(())
    }

    pub async fn create_policy(&self, policy: Policy) -> Result<()> {
        self.policy_store.policies.insert(policy.id, policy.clone());
        
        let mut active = self.policy_store.active_policies.write().await;
        active.push(policy);
        active.sort_by_key(|p| -p.priority);
        
        Ok(())
    }

    pub async fn create_permission(&self, permission: Permission) -> Result<()> {
        self.permission_store.permissions.insert(permission.id, permission.clone());
        
        self.permission_store.resource_permissions
            .entry(permission.resource.clone())
            .or_insert_with(HashSet::new)
            .insert(permission.id);
        
        Ok(())
    }

    pub async fn revoke_role(&self, principal_id: Uuid, role_id: Uuid) -> Result<()> {
        if let Some(mut roles) = self.role_store.principal_roles.get_mut(&principal_id) {
            roles.remove(&role_id);
        }
        Ok(())
    }

    pub async fn delete_policy(&self, policy_id: Uuid) -> Result<()> {
        self.policy_store.policies.remove(&policy_id);
        
        let mut active = self.policy_store.active_policies.write().await;
        active.retain(|p| p.id != policy_id);
        
        Ok(())
    }

    pub async fn get_principal_roles(&self, principal_id: Uuid) -> Result<Vec<Role>> {
        let mut roles = Vec::new();
        
        if let Some(role_ids) = self.role_store.principal_roles.get(&principal_id) {
            for role_id in role_ids.iter() {
                if let Some(role) = self.role_store.roles.get(role_id) {
                    roles.push(role.clone());
                }
            }
        }
        
        Ok(roles)
    }

    pub async fn check_permission(&self, principal_id: Uuid, resource: &str, action: Action) -> Result<bool> {
        let principal = self.load_principal(principal_id).await?;
        
        let context = AuthorizationContext {
            principal,
            resource: resource.to_string(),
            action,
            attributes: HashMap::new(),
            request_time: Utc::now(),
            ip_address: None,
            mfa_verified: false,
        };
        
        self.authorize(context).await
    }

    async fn load_principal(&self, principal_id: Uuid) -> Result<Principal> {
        let roles = self.role_store.principal_roles
            .get(&principal_id)
            .map(|r| r.clone())
            .unwrap_or_default();
        
        Ok(Principal {
            id: principal_id,
            principal_type: PrincipalType::User,
            roles,
            permissions: HashSet::new(),
            attributes: HashMap::new(),
        })
    }
}

impl PolicyEvaluator {
    fn new() -> Self {
        let mut evaluators: DashMap<String, Arc<dyn ConditionEvaluator>> = DashMap::new();
        evaluators.insert("time".to_string(), Arc::new(TimeConditionEvaluator));
        evaluators.insert("ip".to_string(), Arc::new(IpConditionEvaluator));
        evaluators.insert("attribute".to_string(), Arc::new(AttributeConditionEvaluator));
        
        Self {
            condition_evaluators: Arc::new(evaluators),
            resource_matcher: Arc::new(ResourceMatcher::new()),
            action_matcher: Arc::new(ActionMatcher::new()),
        }
    }

    async fn evaluate_condition(&self, condition: &Condition, context: &AuthorizationContext) -> Result<bool> {
        match condition {
            Condition::TimeRange { start, end } => {
                Ok(context.request_time >= *start && context.request_time <= *end)
            }
            Condition::IpRange { cidrs } => {
                if let Some(ip) = &context.ip_address {
                    for cidr in cidrs {
                        if self.ip_in_cidr(ip, cidr)? {
                            return Ok(true);
                        }
                    }
                }
                Ok(false)
            }
            Condition::AttributeMatch { key, value } => {
                Ok(context.attributes.get(key) == Some(value))
            }
            Condition::AttributePattern { key, pattern } => {
                if let Some(val) = context.attributes.get(key) {
                    let re = Regex::new(pattern)?;
                    Ok(re.is_match(val))
                } else {
                    Ok(false)
                }
            }
            Condition::MFA { required } => {
                Ok(!required || context.mfa_verified)
            }
            Condition::ResourceOwner => {
                Ok(context.attributes.get("owner") == Some(&context.principal.id.to_string()))
            }
            Condition::DelegatedBy { principal } => {
                Ok(context.attributes.get("delegator") == Some(principal))
            }
            Condition::Custom { evaluator } => {
                if let Some(eval) = self.condition_evaluators.get(evaluator) {
                    eval.evaluate(condition, context).await
                } else {
                    Ok(false)
                }
            }
        }
    }

    fn ip_in_cidr(&self, ip: &str, cidr: &str) -> Result<bool> {
        Ok(false)
    }
}

struct TimeConditionEvaluator;
struct IpConditionEvaluator;
struct AttributeConditionEvaluator;

#[async_trait]
impl ConditionEvaluator for TimeConditionEvaluator {
    async fn evaluate(&self, condition: &Condition, context: &AuthorizationContext) -> Result<bool> {
        if let Condition::TimeRange { start, end } = condition {
            Ok(context.request_time >= *start && context.request_time <= *end)
        } else {
            Ok(true)
        }
    }
}

#[async_trait]
impl ConditionEvaluator for IpConditionEvaluator {
    async fn evaluate(&self, _condition: &Condition, _context: &AuthorizationContext) -> Result<bool> {
        Ok(true)
    }
}

#[async_trait]
impl ConditionEvaluator for AttributeConditionEvaluator {
    async fn evaluate(&self, condition: &Condition, context: &AuthorizationContext) -> Result<bool> {
        if let Condition::AttributeMatch { key, value } = condition {
            Ok(context.attributes.get(key) == Some(value))
        } else {
            Ok(true)
        }
    }
}

impl RoleStore {
    fn new() -> Self {
        Self {
            roles: Arc::new(DashMap::new()),
            role_hierarchy: Arc::new(RwLock::new(HashMap::new())),
            principal_roles: Arc::new(DashMap::new()),
        }
    }
}

impl PolicyStore {
    fn new() -> Self {
        Self {
            policies: Arc::new(DashMap::new()),
            resource_policies: Arc::new(DashMap::new()),
            active_policies: Arc::new(RwLock::new(Vec::new())),
        }
    }
}

impl PermissionStore {
    fn new() -> Self {
        Self {
            permissions: Arc::new(DashMap::new()),
            resource_permissions: Arc::new(DashMap::new()),
        }
    }
}

impl ResourceMatcher {
    fn new() -> Self {
        Self {
            patterns: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    async fn matches(&self, pattern: &str, resource: &str) -> Result<bool> {
        if pattern == "*" {
            return Ok(true);
        }
        
        if pattern == resource {
            return Ok(true);
        }
        
        if pattern.contains('*') || pattern.contains('?') {
            let regex_pattern = pattern
                .replace('.', "\\.")
                .replace('*', ".*")
                .replace('?', ".");
            
            let re = Regex::new(&format!("^{}$", regex_pattern))?;
            Ok(re.is_match(resource))
        } else {
            Ok(false)
        }
    }
}

impl ActionMatcher {
    fn new() -> Self {
        let mut hierarchy = HashMap::new();
        
        let admin_actions = vec![Action::Read, Action::Write, Action::Delete, Action::Execute, Action::Create, Action::Update, Action::List];
        hierarchy.insert(Action::Admin, admin_actions.into_iter().collect());
        
        let write_actions = vec![Action::Create, Action::Update, Action::Delete];
        hierarchy.insert(Action::Write, write_actions.into_iter().collect());
        
        Self {
            action_hierarchy: Arc::new(RwLock::new(hierarchy)),
        }
    }

    async fn matches(&self, pattern: &str, action: &Action) -> Result<bool> {
        if pattern == "*" {
            return Ok(true);
        }
        
        if pattern == format!("{:?}", action) {
            return Ok(true);
        }
        
        let hierarchy = self.action_hierarchy.read().await;
        if let Ok(pattern_action) = pattern.parse::<Action>() {
            if let Some(children) = hierarchy.get(&pattern_action) {
                return Ok(children.contains(action));
            }
        }
        
        Ok(false)
    }
}

impl AuthorizationCache {
    fn new(ttl: Duration) -> Self {
        Self {
            decisions: Arc::new(DashMap::new()),
            ttl,
        }
    }

    async fn get(&self, key: &str) -> Result<Option<CachedDecision>> {
        if let Some(decision) = self.decisions.get(key) {
            if decision.timestamp + decision.ttl > Utc::now() {
                return Ok(Some(decision.clone()));
            }
            self.decisions.remove(key);
        }
        Ok(None)
    }

    async fn put(&self, key: String, decision: CachedDecision) -> Result<()> {
        self.decisions.insert(key, decision);
        Ok(())
    }
}

impl AuthorizationAudit {
    fn new(config: AuditConfig) -> Self {
        Self {
            logger: Arc::new(DefaultAuditLogger),
            enabled: config.enabled,
            sensitive_resources: config.sensitive_resources,
        }
    }

    async fn log_authorization(&self, event: AuthorizationEvent) -> Result<()> {
        if self.sensitive_resources.contains(&event.resource) || self.enabled {
            self.logger.log_authorization(event).await?;
        }
        Ok(())
    }
}

struct DefaultAuditLogger;

#[async_trait]
impl AuditLogger for DefaultAuditLogger {
    async fn log_authorization(&self, event: AuthorizationEvent) -> Result<()> {
        println!("AUDIT: {:?}", event);
        Ok(())
    }
}

impl AuthorizationMetrics {
    fn new() -> Self {
        Self {
            decisions_total: Arc::new(RwLock::new(0)),
            decisions_allowed: Arc::new(RwLock::new(0)),
            decisions_denied: Arc::new(RwLock::new(0)),
            cache_hits: Arc::new(RwLock::new(0)),
            cache_misses: Arc::new(RwLock::new(0)),
            evaluation_times: Arc::new(RwLock::new(Vec::new())),
        }
    }

    async fn record_decision(&self, allowed: bool, duration_ms: u64) {
        *self.decisions_total.write().await += 1;
        
        if allowed {
            *self.decisions_allowed.write().await += 1;
        } else {
            *self.decisions_denied.write().await += 1;
        }
        
        self.evaluation_times.write().await.push(duration_ms);
    }

    async fn record_cache_hit(&self) {
        *self.cache_hits.write().await += 1;
    }

    async fn record_cache_miss(&self) {
        *self.cache_misses.write().await += 1;
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthorizationConfig {
    pub cache_ttl: Duration,
    pub audit_config: AuditConfig,
    pub max_role_depth: usize,
    pub enable_dynamic_policies: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditConfig {
    pub enabled: bool,
    pub sensitive_resources: HashSet<String>,
    pub log_denied_only: bool,
}

impl fmt::Display for Action {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Action::Read => write!(f, "read"),
            Action::Write => write!(f, "write"),
            Action::Delete => write!(f, "delete"),
            Action::Execute => write!(f, "execute"),
            Action::Create => write!(f, "create"),
            Action::Update => write!(f, "update"),
            Action::List => write!(f, "list"),
            Action::Admin => write!(f, "admin"),
            Action::Custom(s) => write!(f, "{}", s),
        }
    }
}

impl std::str::FromStr for Action {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "read" => Ok(Action::Read),
            "write" => Ok(Action::Write),
            "delete" => Ok(Action::Delete),
            "execute" => Ok(Action::Execute),
            "create" => Ok(Action::Create),
            "update" => Ok(Action::Update),
            "list" => Ok(Action::List),
            "admin" => Ok(Action::Admin),
            _ => Ok(Action::Custom(s.to_string())),
        }
    }
}