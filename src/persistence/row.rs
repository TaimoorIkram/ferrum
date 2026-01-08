use std::fmt::Display;

#[derive(Clone)]
pub struct Row(pub Vec<Option<String>>);

// impl Row {
//     pub fn at(&self, index: usize) -> &Option<String> {
//         self.0.get(index).unwrap()
//     }
// }

impl Display for Row {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let row: Vec<String> = self
            .0
            .iter()
            .map(|value| match value {
                Some(string) => format!("{:16}", string.as_str()),
                None => "NIL".to_string(),
            })
            .collect();
        writeln!(f, "| {} |", row.join(" | "))
    }
}
