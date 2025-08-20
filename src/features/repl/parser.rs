//! Query Parser for Lightning DB REPL
//!
//! Parses REPL commands and queries into structured representations
//! with support for SQL-like syntax, command parameters, and expressions.

use crate::core::error::{Error, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Query parser for REPL commands
pub struct QueryParser {
    /// Parser configuration
    config: ParserConfig,
}

/// Configuration for query parsing
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParserConfig {
    /// Enable SQL-like syntax extensions
    pub enable_sql_extensions: bool,
    /// Enable expression parsing
    pub enable_expressions: bool,
    /// Case sensitivity for commands
    pub case_sensitive_commands: bool,
    /// Maximum query length
    pub max_query_length: usize,
    /// Enable query validation
    pub enable_validation: bool,
}

impl Default for ParserConfig {
    fn default() -> Self {
        Self {
            enable_sql_extensions: true,
            enable_expressions: true,
            case_sensitive_commands: false,
            max_query_length: 10240, // 10KB
            enable_validation: true,
        }
    }
}

/// Parsed command representation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParsedCommand {
    /// Command type
    pub command: CommandType,
    /// Command parameters
    pub parameters: Vec<CommandParameter>,
    /// Optional WHERE clause
    pub where_clause: Option<WhereClause>,
    /// Optional ORDER BY clause
    pub order_by: Option<OrderByClause>,
    /// Optional LIMIT clause
    pub limit: Option<u64>,
    /// Session variables referenced
    pub variables: Vec<String>,
    /// Original query text
    pub original_query: String,
}

/// Types of commands
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum CommandType {
    Get,
    Put,
    Delete,
    Exists,
    Scan,
    Begin,
    Commit,
    Rollback,
    TxGet,
    TxPut,
    TxDelete,
    Info,
    Stats,
    Compact,
    Backup,
    Restore,
    Health,
    Set,
    Show,
    Help,
    Exit,
    Clear,
    History,
    Use,
    Describe,
    Explain,
    // SQL-like extensions
    Select,
    Insert,
    Update,
    Drop,
    Create,
}

/// Command parameter
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommandParameter {
    /// Parameter value
    pub value: ParameterValue,
    /// Parameter type
    pub param_type: ParameterType,
    /// Whether the parameter was quoted
    pub quoted: bool,
}

/// Parameter value types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ParameterValue {
    String(String),
    Integer(i64),
    Float(f64),
    Boolean(bool),
    Null,
    Variable(String),
    Expression(Box<Expression>),
}

/// Parameter types
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ParameterType {
    Key,
    Value,
    Path,
    Number,
    Identifier,
    Literal,
    Variable,
    Expression,
}

/// WHERE clause for filtering
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WhereClause {
    pub condition: Condition,
}

/// ORDER BY clause for sorting
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderByClause {
    pub column: String,
    pub direction: SortDirection,
}

/// Sort direction
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SortDirection {
    Asc,
    Desc,
}

/// Condition for WHERE clauses
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Condition {
    Equals {
        column: String,
        value: ParameterValue,
    },
    NotEquals {
        column: String,
        value: ParameterValue,
    },
    GreaterThan {
        column: String,
        value: ParameterValue,
    },
    LessThan {
        column: String,
        value: ParameterValue,
    },
    Like {
        column: String,
        pattern: String,
    },
    In {
        column: String,
        values: Vec<ParameterValue>,
    },
    IsNull {
        column: String,
    },
    IsNotNull {
        column: String,
    },
    And {
        left: Box<Condition>,
        right: Box<Condition>,
    },
    Or {
        left: Box<Condition>,
        right: Box<Condition>,
    },
    Not {
        condition: Box<Condition>,
    },
}

/// Expression for computed values
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Expression {
    Literal(ParameterValue),
    Variable(String),
    Function {
        name: String,
        args: Vec<Expression>,
    },
    Binary {
        op: BinaryOperator,
        left: Box<Expression>,
        right: Box<Expression>,
    },
    Unary {
        op: UnaryOperator,
        expr: Box<Expression>,
    },
}

/// Binary operators
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BinaryOperator {
    Add,
    Subtract,
    Multiply,
    Divide,
    Modulo,
    Concat,
}

/// Unary operators
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum UnaryOperator {
    Plus,
    Minus,
    Not,
}

/// Abstract Syntax Tree for complex queries
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryAst {
    /// Root node
    pub root: AstNode,
    /// Symbol table for variables
    pub symbols: HashMap<String, ParameterValue>,
}

/// AST node
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AstNode {
    Command(ParsedCommand),
    Block(Vec<AstNode>),
    If {
        condition: Condition,
        then_block: Box<AstNode>,
        else_block: Option<Box<AstNode>>,
    },
    Loop {
        condition: Condition,
        body: Box<AstNode>,
    },
    Assignment {
        variable: String,
        value: Expression,
    },
}

/// Token for lexical analysis
#[derive(Debug, Clone, PartialEq)]
struct Token {
    token_type: TokenType,
    value: String,
    position: usize,
}

/// Token types
#[derive(Debug, Clone, PartialEq)]
enum TokenType {
    Keyword,
    Identifier,
    String,
    Number,
    Float,
    Boolean,
    Null,
    Variable,
    Operator,
    Punctuation,
    Whitespace,
    Comment,
    EndOfInput,
}

impl QueryParser {
    /// Create a new query parser
    pub fn new() -> Self {
        Self {
            config: ParserConfig::default(),
        }
    }

    /// Create parser with custom configuration
    pub fn with_config(config: ParserConfig) -> Self {
        Self { config }
    }

    /// Parse a query string into a structured command
    pub fn parse(&self, query: &str) -> Result<ParsedCommand> {
        if query.len() > self.config.max_query_length {
            return Err(Error::Generic(format!(
                "Query too long: {} characters (max: {})",
                query.len(),
                self.config.max_query_length
            )));
        }

        let tokens = self.tokenize(query)?;
        let mut parser_state = ParserState::new(tokens);

        self.parse_command(&mut parser_state, query)
    }

    /// Parse a complex query into an AST
    pub fn parse_ast(&self, query: &str) -> Result<QueryAst> {
        let tokens = self.tokenize(query)?;
        let mut parser_state = ParserState::new(tokens);

        let root = self.parse_statement(&mut parser_state)?;

        Ok(QueryAst {
            root,
            symbols: parser_state.symbols,
        })
    }

    /// Tokenize input query
    fn tokenize(&self, query: &str) -> Result<Vec<Token>> {
        let mut tokens = Vec::new();
        let mut chars = query.char_indices().peekable();

        while let Some((pos, ch)) = chars.next() {
            match ch {
                // Whitespace
                ' ' | '\t' | '\n' | '\r' => {
                    let mut value = String::from(ch);
                    while let Some(&(_, next_ch)) = chars.peek() {
                        if next_ch.is_whitespace() {
                            value.push(next_ch);
                            chars.next();
                        } else {
                            break;
                        }
                    }
                    tokens.push(Token {
                        token_type: TokenType::Whitespace,
                        value,
                        position: pos,
                    });
                }

                // String literals
                '"' | '\'' => {
                    let quote_char = ch;
                    let mut value = String::new();
                    let mut escaped = false;

                    while let Some((_, next_ch)) = chars.next() {
                        if escaped {
                            match next_ch {
                                'n' => value.push('\n'),
                                't' => value.push('\t'),
                                'r' => value.push('\r'),
                                '\\' => value.push('\\'),
                                '\'' => value.push('\''),
                                '"' => value.push('"'),
                                _ => {
                                    value.push('\\');
                                    value.push(next_ch);
                                }
                            }
                            escaped = false;
                        } else if next_ch == '\\' {
                            escaped = true;
                        } else if next_ch == quote_char {
                            break;
                        } else {
                            value.push(next_ch);
                        }
                    }

                    tokens.push(Token {
                        token_type: TokenType::String,
                        value,
                        position: pos,
                    });
                }

                // Numbers
                '0'..='9' => {
                    let mut value = String::from(ch);
                    let mut is_float = false;

                    while let Some(&(_, next_ch)) = chars.peek() {
                        if next_ch.is_ascii_digit() {
                            value.push(next_ch);
                            chars.next();
                        } else if next_ch == '.' && !is_float {
                            is_float = true;
                            value.push(next_ch);
                            chars.next();
                        } else {
                            break;
                        }
                    }

                    tokens.push(Token {
                        token_type: if is_float {
                            TokenType::Float
                        } else {
                            TokenType::Number
                        },
                        value,
                        position: pos,
                    });
                }

                // Variables (starting with $)
                '$' => {
                    let mut value = String::new();

                    while let Some(&(_, next_ch)) = chars.peek() {
                        if next_ch.is_alphanumeric() || next_ch == '_' {
                            value.push(next_ch);
                            chars.next();
                        } else {
                            break;
                        }
                    }

                    tokens.push(Token {
                        token_type: TokenType::Variable,
                        value,
                        position: pos,
                    });
                }

                // Comments
                '#' => {
                    let mut value = String::from(ch);

                    while let Some((_, next_ch)) = chars.next() {
                        if next_ch == '\n' || next_ch == '\r' {
                            break;
                        }
                        value.push(next_ch);
                    }

                    tokens.push(Token {
                        token_type: TokenType::Comment,
                        value,
                        position: pos,
                    });
                }

                // Multi-character operators
                '=' | '!' | '<' | '>' => {
                    let mut value = String::from(ch);

                    if let Some(&(_, next_ch)) = chars.peek() {
                        if (ch == '=' && next_ch == '=')
                            || (ch == '!' && next_ch == '=')
                            || (ch == '<' && next_ch == '=')
                            || (ch == '>' && next_ch == '=')
                        {
                            value.push(next_ch);
                            chars.next();
                        }
                    }

                    tokens.push(Token {
                        token_type: TokenType::Operator,
                        value,
                        position: pos,
                    });
                }

                // Single-character operators and punctuation
                '+' | '-' | '*' | '/' | '%' | '&' | '|' | '^' | '(' | ')' | '[' | ']' | '{'
                | '}' | ',' | ';' => {
                    tokens.push(Token {
                        token_type: if "+-*/%&|^".contains(ch) {
                            TokenType::Operator
                        } else {
                            TokenType::Punctuation
                        },
                        value: ch.to_string(),
                        position: pos,
                    });
                }

                // Identifiers and keywords
                'a'..='z' | 'A'..='Z' | '_' => {
                    let mut value = String::from(ch);

                    while let Some(&(_, next_ch)) = chars.peek() {
                        if next_ch.is_alphanumeric() || next_ch == '_' {
                            value.push(next_ch);
                            chars.next();
                        } else {
                            break;
                        }
                    }

                    let token_type = match value.to_uppercase().as_str() {
                        "GET" | "PUT" | "DELETE" | "EXISTS" | "SCAN" | "BEGIN" | "COMMIT"
                        | "ROLLBACK" | "TXGET" | "TXPUT" | "TXDELETE" | "INFO" | "STATS"
                        | "COMPACT" | "BACKUP" | "RESTORE" | "HEALTH" | "SET" | "SHOW" | "HELP"
                        | "EXIT" | "QUIT" | "CLEAR" | "HISTORY" | "SELECT" | "INSERT"
                        | "UPDATE" | "DROP" | "CREATE" | "WHERE" | "ORDER" | "BY" | "LIMIT"
                        | "ASC" | "DESC" | "AND" | "OR" | "NOT" | "LIKE" | "IN" | "IS" | "IF"
                        | "THEN" | "ELSE" | "WHILE" | "FOR" | "DO" => TokenType::Keyword,
                        "TRUE" | "FALSE" => TokenType::Boolean,
                        "NULL" => TokenType::Null,
                        _ => TokenType::Identifier,
                    };

                    tokens.push(Token {
                        token_type,
                        value,
                        position: pos,
                    });
                }

                // Unknown character
                _ => {
                    return Err(Error::Generic(format!(
                        "Unexpected character '{}' at position {}",
                        ch, pos
                    )));
                }
            }
        }

        tokens.push(Token {
            token_type: TokenType::EndOfInput,
            value: String::new(),
            position: query.len(),
        });

        Ok(tokens)
    }

    /// Parse a command from tokens
    fn parse_command(
        &self,
        state: &mut ParserState,
        original_query: &str,
    ) -> Result<ParsedCommand> {
        // Skip whitespace and comments
        state.skip_whitespace_and_comments();

        if state.is_at_end() {
            return Err(Error::Generic("Empty query".to_string()));
        }

        let command_token = state.consume()?;
        if command_token.token_type != TokenType::Keyword {
            return Err(Error::Generic(format!(
                "Expected command, found '{}'",
                command_token.value
            )));
        }

        let command = self.parse_command_type(&command_token.value)?;
        let mut parameters = Vec::new();
        let mut where_clause = None;
        let mut order_by = None;
        let mut limit = None;
        let mut variables = Vec::new();

        // Parse command-specific parameters
        match command {
            CommandType::Get | CommandType::Delete | CommandType::Exists => {
                state.skip_whitespace_and_comments();
                if !state.is_at_end() && !self.is_clause_keyword(&state.peek()?.value) {
                    let key_param = self.parse_parameter(state, ParameterType::Key)?;
                    if let ParameterValue::Variable(ref var) = key_param.value {
                        variables.push(var.clone());
                    }
                    parameters.push(key_param);
                }
            }

            CommandType::Put => {
                state.skip_whitespace_and_comments();
                if !state.is_at_end() && !self.is_clause_keyword(&state.peek()?.value) {
                    let key_param = self.parse_parameter(state, ParameterType::Key)?;
                    if let ParameterValue::Variable(ref var) = key_param.value {
                        variables.push(var.clone());
                    }
                    parameters.push(key_param);

                    state.skip_whitespace_and_comments();
                    if !state.is_at_end() && !self.is_clause_keyword(&state.peek()?.value) {
                        let value_param = self.parse_parameter(state, ParameterType::Value)?;
                        if let ParameterValue::Variable(ref var) = value_param.value {
                            variables.push(var.clone());
                        }
                        parameters.push(value_param);
                    }
                }
            }

            CommandType::Scan => {
                state.skip_whitespace_and_comments();
                if !state.is_at_end() && !self.is_clause_keyword(&state.peek()?.value) {
                    // Optional prefix parameter
                    let prefix_param = self.parse_parameter(state, ParameterType::Key)?;
                    if let ParameterValue::Variable(ref var) = prefix_param.value {
                        variables.push(var.clone());
                    }
                    parameters.push(prefix_param);

                    state.skip_whitespace_and_comments();
                    if !state.is_at_end() && !self.is_clause_keyword(&state.peek()?.value) {
                        // Optional limit parameter
                        let limit_param = self.parse_parameter(state, ParameterType::Number)?;
                        if let ParameterValue::Variable(ref var) = limit_param.value {
                            variables.push(var.clone());
                        }
                        parameters.push(limit_param);
                    }
                }
            }

            CommandType::Set => {
                state.skip_whitespace_and_comments();
                if !state.is_at_end() {
                    let var_param = self.parse_parameter(state, ParameterType::Identifier)?;
                    parameters.push(var_param);

                    state.skip_whitespace_and_comments();
                    if !state.is_at_end() {
                        let value_param = self.parse_parameter(state, ParameterType::Literal)?;
                        if let ParameterValue::Variable(ref var) = value_param.value {
                            variables.push(var.clone());
                        }
                        parameters.push(value_param);
                    }
                }
            }

            CommandType::Backup | CommandType::Restore => {
                state.skip_whitespace_and_comments();
                if !state.is_at_end() && !self.is_clause_keyword(&state.peek()?.value) {
                    let path_param = self.parse_parameter(state, ParameterType::Path)?;
                    if let ParameterValue::Variable(ref var) = path_param.value {
                        variables.push(var.clone());
                    }
                    parameters.push(path_param);
                }
            }

            _ => {
                // Parse any remaining parameters
                while !state.is_at_end() && !self.is_clause_keyword(&state.peek()?.value) {
                    state.skip_whitespace_and_comments();
                    if state.is_at_end() {
                        break;
                    }

                    let param = self.parse_parameter(state, ParameterType::Literal)?;
                    if let ParameterValue::Variable(ref var) = param.value {
                        variables.push(var.clone());
                    }
                    parameters.push(param);
                }
            }
        }

        // Parse optional clauses
        while !state.is_at_end() {
            state.skip_whitespace_and_comments();
            if state.is_at_end() {
                break;
            }

            let clause_token = state.peek()?;
            match clause_token.value.to_uppercase().as_str() {
                "WHERE" => {
                    state.consume()?; // consume WHERE keyword
                    where_clause = Some(self.parse_where_clause(state)?);
                }
                "ORDER" => {
                    state.consume()?; // consume ORDER keyword
                    state.skip_whitespace_and_comments();

                    let by_token = state.consume()?;
                    if by_token.value.to_uppercase() != "BY" {
                        return Err(Error::Generic("Expected BY after ORDER".to_string()));
                    }

                    order_by = Some(self.parse_order_by_clause(state)?);
                }
                "LIMIT" => {
                    state.consume()?; // consume LIMIT keyword
                    state.skip_whitespace_and_comments();

                    let limit_param = self.parse_parameter(state, ParameterType::Number)?;
                    match limit_param.value {
                        ParameterValue::Integer(n) => limit = Some(n as u64),
                        _ => return Err(Error::Generic("LIMIT must be a number".to_string())),
                    }
                }
                _ => break,
            }
        }

        Ok(ParsedCommand {
            command,
            parameters,
            where_clause,
            order_by,
            limit,
            variables,
            original_query: original_query.to_string(),
        })
    }

    /// Parse a statement for AST
    fn parse_statement(&self, state: &mut ParserState) -> Result<AstNode> {
        state.skip_whitespace_and_comments();

        if state.is_at_end() {
            return Err(Error::Generic("Empty statement".to_string()));
        }

        let token_value = state.peek()?.value.clone();
        match token_value.to_uppercase().as_str() {
            "IF" => self.parse_if_statement(state),
            "WHILE" => self.parse_while_statement(state),
            _ => {
                let command = self.parse_command(state, &token_value)?;
                Ok(AstNode::Command(command))
            }
        }
    }

    /// Parse command type from string
    fn parse_command_type(&self, cmd: &str) -> Result<CommandType> {
        let cmd_upper = if self.config.case_sensitive_commands {
            cmd.to_string()
        } else {
            cmd.to_uppercase()
        };

        match cmd_upper.as_str() {
            "GET" => Ok(CommandType::Get),
            "PUT" => Ok(CommandType::Put),
            "DELETE" => Ok(CommandType::Delete),
            "EXISTS" => Ok(CommandType::Exists),
            "SCAN" => Ok(CommandType::Scan),
            "BEGIN" => Ok(CommandType::Begin),
            "COMMIT" => Ok(CommandType::Commit),
            "ROLLBACK" => Ok(CommandType::Rollback),
            "TXGET" => Ok(CommandType::TxGet),
            "TXPUT" => Ok(CommandType::TxPut),
            "TXDELETE" => Ok(CommandType::TxDelete),
            "INFO" => Ok(CommandType::Info),
            "STATS" => Ok(CommandType::Stats),
            "COMPACT" => Ok(CommandType::Compact),
            "BACKUP" => Ok(CommandType::Backup),
            "RESTORE" => Ok(CommandType::Restore),
            "HEALTH" => Ok(CommandType::Health),
            "SET" => Ok(CommandType::Set),
            "SHOW" => Ok(CommandType::Show),
            "HELP" => Ok(CommandType::Help),
            "EXIT" | "QUIT" => Ok(CommandType::Exit),
            "CLEAR" => Ok(CommandType::Clear),
            "HISTORY" => Ok(CommandType::History),
            "SELECT" => Ok(CommandType::Select),
            "INSERT" => Ok(CommandType::Insert),
            "UPDATE" => Ok(CommandType::Update),
            "DROP" => Ok(CommandType::Drop),
            "CREATE" => Ok(CommandType::Create),
            _ => Err(Error::Generic(format!("Unknown command: {}", cmd))),
        }
    }

    /// Parse a parameter
    fn parse_parameter(
        &self,
        state: &mut ParserState,
        expected_type: ParameterType,
    ) -> Result<CommandParameter> {
        state.skip_whitespace_and_comments();

        let token = state.consume()?;
        let (value, quoted) = match token.token_type {
            TokenType::String => (ParameterValue::String(token.value), true),
            TokenType::Number => {
                let num = token
                    .value
                    .parse::<i64>()
                    .map_err(|_| Error::Generic(format!("Invalid number: {}", token.value)))?;
                (ParameterValue::Integer(num), false)
            }
            TokenType::Float => {
                let num = token
                    .value
                    .parse::<f64>()
                    .map_err(|_| Error::Generic(format!("Invalid float: {}", token.value)))?;
                (ParameterValue::Float(num), false)
            }
            TokenType::Boolean => {
                let bool_val = token.value.to_uppercase() == "TRUE";
                (ParameterValue::Boolean(bool_val), false)
            }
            TokenType::Null => (ParameterValue::Null, false),
            TokenType::Variable => (ParameterValue::Variable(token.value), false),
            TokenType::Identifier => (ParameterValue::String(token.value), false),
            _ => return Err(Error::Generic(format!("Unexpected token: {}", token.value))),
        };

        Ok(CommandParameter {
            value,
            param_type: expected_type,
            quoted,
        })
    }

    /// Parse WHERE clause
    fn parse_where_clause(&self, state: &mut ParserState) -> Result<WhereClause> {
        state.skip_whitespace_and_comments();
        let condition = self.parse_condition(state)?;
        Ok(WhereClause { condition })
    }

    /// Parse ORDER BY clause
    fn parse_order_by_clause(&self, state: &mut ParserState) -> Result<OrderByClause> {
        state.skip_whitespace_and_comments();

        let column_token = state.consume()?;
        let column = column_token.value;

        state.skip_whitespace_and_comments();
        let direction = if !state.is_at_end() {
            let dir_token = state.peek()?;
            match dir_token.value.to_uppercase().as_str() {
                "ASC" => {
                    state.consume()?;
                    SortDirection::Asc
                }
                "DESC" => {
                    state.consume()?;
                    SortDirection::Desc
                }
                _ => SortDirection::Asc,
            }
        } else {
            SortDirection::Asc
        };

        Ok(OrderByClause { column, direction })
    }

    /// Parse condition for WHERE clause
    fn parse_condition(&self, state: &mut ParserState) -> Result<Condition> {
        self.parse_or_condition(state)
    }

    /// Parse OR condition
    fn parse_or_condition(&self, state: &mut ParserState) -> Result<Condition> {
        let mut left = self.parse_and_condition(state)?;

        while !state.is_at_end() {
            state.skip_whitespace_and_comments();
            if state.is_at_end() {
                break;
            }

            let token = state.peek()?;
            if token.value.to_uppercase() == "OR" {
                state.consume()?;
                let right = self.parse_and_condition(state)?;
                left = Condition::Or {
                    left: Box::new(left),
                    right: Box::new(right),
                };
            } else {
                break;
            }
        }

        Ok(left)
    }

    /// Parse AND condition
    fn parse_and_condition(&self, state: &mut ParserState) -> Result<Condition> {
        let mut left = self.parse_not_condition(state)?;

        while !state.is_at_end() {
            state.skip_whitespace_and_comments();
            if state.is_at_end() {
                break;
            }

            let token = state.peek()?;
            if token.value.to_uppercase() == "AND" {
                state.consume()?;
                let right = self.parse_not_condition(state)?;
                left = Condition::And {
                    left: Box::new(left),
                    right: Box::new(right),
                };
            } else {
                break;
            }
        }

        Ok(left)
    }

    /// Parse NOT condition
    fn parse_not_condition(&self, state: &mut ParserState) -> Result<Condition> {
        state.skip_whitespace_and_comments();

        let token = state.peek()?;
        if token.value.to_uppercase() == "NOT" {
            state.consume()?;
            let condition = self.parse_primary_condition(state)?;
            Ok(Condition::Not {
                condition: Box::new(condition),
            })
        } else {
            self.parse_primary_condition(state)
        }
    }

    /// Parse primary condition
    fn parse_primary_condition(&self, state: &mut ParserState) -> Result<Condition> {
        state.skip_whitespace_and_comments();

        let column_token = state.consume()?;
        let column = column_token.value;

        state.skip_whitespace_and_comments();
        let op_token = state.consume()?;

        match op_token.value.as_str() {
            "=" | "==" => {
                let value_param = self.parse_parameter(state, ParameterType::Literal)?;
                Ok(Condition::Equals {
                    column,
                    value: value_param.value,
                })
            }
            "!=" | "<>" => {
                let value_param = self.parse_parameter(state, ParameterType::Literal)?;
                Ok(Condition::NotEquals {
                    column,
                    value: value_param.value,
                })
            }
            ">" => {
                let value_param = self.parse_parameter(state, ParameterType::Literal)?;
                Ok(Condition::GreaterThan {
                    column,
                    value: value_param.value,
                })
            }
            "<" => {
                let value_param = self.parse_parameter(state, ParameterType::Literal)?;
                Ok(Condition::LessThan {
                    column,
                    value: value_param.value,
                })
            }
            _ => Err(Error::Generic(format!(
                "Unsupported operator: {}",
                op_token.value
            ))),
        }
    }

    /// Parse IF statement for AST
    fn parse_if_statement(&self, state: &mut ParserState) -> Result<AstNode> {
        state.consume()?; // consume IF keyword

        let condition = self.parse_condition(state)?;

        state.skip_whitespace_and_comments();
        let then_token = state.consume()?;
        if then_token.value.to_uppercase() != "THEN" {
            return Err(Error::Generic(
                "Expected THEN after IF condition".to_string(),
            ));
        }

        let then_block = Box::new(self.parse_statement(state)?);

        let else_block = if !state.is_at_end() {
            state.skip_whitespace_and_comments();
            let token = state.peek()?;
            if token.value.to_uppercase() == "ELSE" {
                state.consume()?;
                Some(Box::new(self.parse_statement(state)?))
            } else {
                None
            }
        } else {
            None
        };

        Ok(AstNode::If {
            condition,
            then_block,
            else_block,
        })
    }

    /// Parse WHILE statement for AST
    fn parse_while_statement(&self, state: &mut ParserState) -> Result<AstNode> {
        state.consume()?; // consume WHILE keyword

        let condition = self.parse_condition(state)?;

        state.skip_whitespace_and_comments();
        let do_token = state.consume()?;
        if do_token.value.to_uppercase() != "DO" {
            return Err(Error::Generic(
                "Expected DO after WHILE condition".to_string(),
            ));
        }

        let body = Box::new(self.parse_statement(state)?);

        Ok(AstNode::Loop { condition, body })
    }

    /// Check if a token is a clause keyword
    fn is_clause_keyword(&self, token_value: &str) -> bool {
        matches!(
            token_value.to_uppercase().as_str(),
            "WHERE" | "ORDER" | "LIMIT"
        )
    }
}

/// Parser state for tracking position and context
struct ParserState {
    tokens: Vec<Token>,
    position: usize,
    symbols: HashMap<String, ParameterValue>,
}

impl ParserState {
    fn new(tokens: Vec<Token>) -> Self {
        Self {
            tokens,
            position: 0,
            symbols: HashMap::new(),
        }
    }

    fn is_at_end(&self) -> bool {
        self.position >= self.tokens.len()
            || self.tokens[self.position].token_type == TokenType::EndOfInput
    }

    fn peek(&self) -> Result<&Token> {
        if self.is_at_end() {
            Err(Error::Generic("Unexpected end of input".to_string()))
        } else {
            Ok(&self.tokens[self.position])
        }
    }

    fn consume(&mut self) -> Result<Token> {
        if self.is_at_end() {
            Err(Error::Generic("Unexpected end of input".to_string()))
        } else {
            let token = self.tokens[self.position].clone();
            self.position += 1;
            Ok(token)
        }
    }

    fn skip_whitespace_and_comments(&mut self) {
        while !self.is_at_end() {
            let token = &self.tokens[self.position];
            if matches!(token.token_type, TokenType::Whitespace | TokenType::Comment) {
                self.position += 1;
            } else {
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parser_creation() {
        let parser = QueryParser::new();
        assert!(!parser.config.case_sensitive_commands);
    }

    #[test]
    fn test_tokenize_simple_command() {
        let parser = QueryParser::new();
        let tokens = parser.tokenize("GET key1").unwrap();

        assert_eq!(tokens.len(), 4); // GET, whitespace, key1, EOF
        assert_eq!(tokens[0].token_type, TokenType::Keyword);
        assert_eq!(tokens[0].value, "GET");
        assert_eq!(tokens[2].token_type, TokenType::Identifier);
        assert_eq!(tokens[2].value, "key1");
    }

    #[test]
    fn test_tokenize_string_literal() {
        let parser = QueryParser::new();
        let tokens = parser
            .tokenize("PUT \"key with spaces\" \"value\"")
            .unwrap();

        let string_tokens: Vec<_> = tokens
            .iter()
            .filter(|t| t.token_type == TokenType::String)
            .collect();

        assert_eq!(string_tokens.len(), 2);
        assert_eq!(string_tokens[0].value, "key with spaces");
        assert_eq!(string_tokens[1].value, "value");
    }

    #[test]
    fn test_tokenize_numbers() {
        let parser = QueryParser::new();
        let tokens = parser.tokenize("123 45.67").unwrap();

        let number_tokens: Vec<_> = tokens
            .iter()
            .filter(|t| matches!(t.token_type, TokenType::Number | TokenType::Float))
            .collect();

        assert_eq!(number_tokens.len(), 2);
        assert_eq!(number_tokens[0].token_type, TokenType::Number);
        assert_eq!(number_tokens[0].value, "123");
        assert_eq!(number_tokens[1].token_type, TokenType::Float);
        assert_eq!(number_tokens[1].value, "45.67");
    }

    #[test]
    fn test_parse_simple_get_command() {
        let parser = QueryParser::new();
        let command = parser.parse("GET mykey").unwrap();

        assert_eq!(command.command, CommandType::Get);
        assert_eq!(command.parameters.len(), 1);
        assert_eq!(command.parameters[0].param_type, ParameterType::Key);

        if let ParameterValue::String(key) = &command.parameters[0].value {
            assert_eq!(key, "mykey");
        } else {
            panic!("Expected string parameter");
        }
    }

    #[test]
    fn test_parse_put_command() {
        let parser = QueryParser::new();
        let command = parser.parse("PUT mykey myvalue").unwrap();

        assert_eq!(command.command, CommandType::Put);
        assert_eq!(command.parameters.len(), 2);
        assert_eq!(command.parameters[0].param_type, ParameterType::Key);
        assert_eq!(command.parameters[1].param_type, ParameterType::Value);
    }

    #[test]
    fn test_parse_command_with_where_clause() {
        let parser = QueryParser::new();
        let command = parser.parse("SCAN WHERE key = \"test\"").unwrap();

        assert_eq!(command.command, CommandType::Scan);
        assert!(command.where_clause.is_some());

        if let Some(where_clause) = command.where_clause {
            if let Condition::Equals { column, value } = where_clause.condition {
                assert_eq!(column, "key");
                if let ParameterValue::String(val) = value {
                    assert_eq!(val, "test");
                } else {
                    panic!("Expected string value in WHERE clause");
                }
            } else {
                panic!("Expected Equals condition");
            }
        }
    }

    #[test]
    fn test_parse_command_with_limit() {
        let parser = QueryParser::new();
        let command = parser.parse("SCAN LIMIT 10").unwrap();

        assert_eq!(command.command, CommandType::Scan);
        assert_eq!(command.limit, Some(10));
    }

    #[test]
    fn test_parse_variable_reference() {
        let parser = QueryParser::new();
        let command = parser.parse("GET $myvar").unwrap();

        assert_eq!(command.variables.len(), 1);
        assert_eq!(command.variables[0], "myvar");
    }

    #[test]
    fn test_parse_error_handling() {
        let parser = QueryParser::new();

        // Empty query
        assert!(parser.parse("").is_err());

        // Unknown command
        assert!(parser.parse("UNKNOWN_COMMAND").is_err());

        // Invalid syntax
        assert!(parser.parse("GET WHERE").is_err());
    }

    #[test]
    fn test_case_insensitive_commands() {
        let parser = QueryParser::new();

        let command1 = parser.parse("get mykey").unwrap();
        let command2 = parser.parse("GET mykey").unwrap();
        let command3 = parser.parse("Get mykey").unwrap();

        assert_eq!(command1.command, CommandType::Get);
        assert_eq!(command2.command, CommandType::Get);
        assert_eq!(command3.command, CommandType::Get);
    }

    #[test]
    fn test_complex_where_condition() {
        let parser = QueryParser::new();
        let command = parser
            .parse("SCAN WHERE key = \"test\" AND value > 100")
            .unwrap();

        assert!(command.where_clause.is_some());

        if let Some(where_clause) = command.where_clause {
            if let Condition::And { left, right } = where_clause.condition {
                assert!(matches!(*left, Condition::Equals { .. }));
                assert!(matches!(*right, Condition::GreaterThan { .. }));
            } else {
                panic!("Expected AND condition");
            }
        }
    }
}
