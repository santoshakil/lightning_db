pub trait Tokenizer: Send + Sync {
    fn tokenize(&self, text: &str) -> TokenStream;
}

pub struct TokenStream {
    tokens: Vec<Token>,
    position: usize,
}

pub struct Token {
    pub text: String,
    pub position: usize,
    pub offset: usize,
    pub length: usize,
    pub token_type: TokenType,
}

#[derive(Debug, Clone, Copy)]
pub enum TokenType {
    Word,
    Number,
    Symbol,
    Whitespace,
    Punctuation,
}

impl TokenStream {
    pub fn new(tokens: Vec<Token>) -> Self {
        Self { tokens, position: 0 }
    }
    
    pub fn next(&mut self) -> Option<&Token> {
        if self.position < self.tokens.len() {
            let token = &self.tokens[self.position];
            self.position += 1;
            Some(token)
        } else {
            None
        }
    }
}