pub struct Highlighter {
    options: HighlightOptions,
}

pub struct HighlightOptions {
    pub pre_tag: String,
    pub post_tag: String,
    pub fragment_size: usize,
    pub number_of_fragments: usize,
}

pub struct Snippet {
    pub text: String,
    pub score: f32,
    pub highlights: Vec<HighlightSpan>,
}

pub struct HighlightSpan {
    pub start: usize,
    pub end: usize,
    pub term: String,
}

impl Highlighter {
    pub fn new(options: HighlightOptions) -> Self {
        Self { options }
    }
    
    pub fn highlight(&self, text: &str, terms: &[String]) -> Vec<Snippet> {
        let mut snippets = Vec::new();
        
        for term in terms {
            if let Some(pos) = text.find(term) {
                let highlighted = format!(
                    "{}{}{}{}{}",
                    &text[..pos],
                    self.options.pre_tag,
                    term,
                    self.options.post_tag,
                    &text[pos + term.len()..]
                );
                
                snippets.push(Snippet {
                    text: highlighted,
                    score: 1.0,
                    highlights: vec![HighlightSpan {
                        start: pos,
                        end: pos + term.len(),
                        term: term.clone(),
                    }],
                });
            }
        }
        
        snippets
    }
}

impl Default for HighlightOptions {
    fn default() -> Self {
        Self {
            pre_tag: "<em>".to_string(),
            post_tag: "</em>".to_string(),
            fragment_size: 150,
            number_of_fragments: 3,
        }
    }
}