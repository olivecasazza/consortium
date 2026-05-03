//! Generic tree visualization for consortium CLIs.
//!
//! Mirrors `tree -L N` and `nh`-style output: depth-limited box-drawing
//! tree with optional colored status badges. Plus machine-friendly
//! formats — JSON, YAML, TOML — emitted from the same data model so
//! `--format json` / `--format tree` produce different surface
//! renderings of the same underlying tree.
//!
//! ## Usage
//!
//! Implement [`TreeNode`] for whatever you want to visualize. The
//! renderer walks the tree generically.
//!
//! ```
//! use consortium_cli::tree::{render, NodeStatus, OutputFormat, TreeNode};
//!
//! struct Pkg {
//!     name: String,
//!     status: NodeStatus,
//!     deps: Vec<Pkg>,
//! }
//!
//! impl TreeNode for Pkg {
//!     fn label(&self) -> String { self.name.clone() }
//!     fn status(&self) -> Option<NodeStatus> { Some(self.status.clone()) }
//!     fn children(&self) -> Vec<&dyn TreeNode> {
//!         self.deps.iter().map(|c| c as &dyn TreeNode).collect()
//!     }
//! }
//!
//! let root = Pkg {
//!     name: "your-system".into(),
//!     status: NodeStatus::Ok,
//!     deps: vec![Pkg {
//!         name: "hello-2.12.1".into(),
//!         status: NodeStatus::InProgress,
//!         deps: vec![],
//!     }],
//! };
//!
//! // tree -L 2 with colors
//! let txt = render(&root, &OutputFormat::Tree {
//!     max_depth: Some(2),
//!     color: true,
//! });
//!
//! // machine output
//! let json = render(&root, &OutputFormat::Json);
//! let yaml = render(&root, &OutputFormat::Yaml);
//! let toml = render(&root, &OutputFormat::Toml);
//! ```

use std::collections::BTreeMap;

use console::Style;

// ============================================================================
// Trait + types
// ============================================================================

/// A node in a tree. Implementors provide a label, optional status,
/// optional metadata key/value pairs (rendered as a sub-line under the
/// node label), and a list of children.
pub trait TreeNode {
    fn label(&self) -> String;

    fn status(&self) -> Option<NodeStatus> {
        None
    }

    /// Extra `(key, value)` pairs rendered inline after the label
    /// (e.g. `"r0", "12ms"` → ` [r0=12ms]`). Useful for round IDs,
    /// durations, sizes — anything that helps reading the tree.
    fn metadata(&self) -> Vec<(String, String)> {
        Vec::new()
    }

    fn children(&self) -> Vec<&dyn TreeNode>;
}

/// Status badge rendered next to a node. `Custom` lets callers add
/// domain-specific badges without bloating this enum.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeStatus {
    /// Built / converged / done — green ✓
    Ok,
    /// Currently running — cyan →
    InProgress,
    /// Not started yet — dim ⏵
    Pending,
    /// Errored — red ✗
    Failed,
    /// Custom badge text + optional color name (`"yellow"`, `"magenta"`, ...).
    Custom {
        glyph: String,
        color: Option<String>,
    },
}

impl NodeStatus {
    /// Glyphs match nix-output-monitor (nom)'s set, which `nh` uses
    /// downstream. Kept verbatim so the visualization reads identical
    /// to anyone familiar with `nh`/`nom`.
    fn glyph(&self) -> &str {
        match self {
            NodeStatus::Ok => "✔",         // U+2714 heavy check mark
            NodeStatus::InProgress => "⏵", // U+23F5 black medium right-pointing triangle
            NodeStatus::Pending => "⏸",    // U+23F8 double vertical bar (paused/planned)
            NodeStatus::Failed => "⚠",     // U+26A0 warning sign (matches nom; was ✗)
            NodeStatus::Custom { glyph, .. } => glyph,
        }
    }

    /// Colors match nom's palette: yellow+bold for running, plain green
    /// for done, plain blue for planned, red+bold for failed.
    fn style(&self) -> Style {
        match self {
            NodeStatus::Ok => Style::new().green(),
            NodeStatus::InProgress => Style::new().yellow().bold(),
            NodeStatus::Pending => Style::new().blue(),
            NodeStatus::Failed => Style::new().red().bold(),
            NodeStatus::Custom { color, .. } => match color.as_deref() {
                Some("red") => Style::new().red(),
                Some("green") => Style::new().green(),
                Some("yellow") => Style::new().yellow(),
                Some("blue") => Style::new().blue(),
                Some("magenta") => Style::new().magenta(),
                Some("cyan") => Style::new().cyan(),
                _ => Style::new(),
            },
        }
    }

    fn as_str(&self) -> &str {
        match self {
            NodeStatus::Ok => "ok",
            NodeStatus::InProgress => "in-progress",
            NodeStatus::Pending => "pending",
            NodeStatus::Failed => "failed",
            NodeStatus::Custom { glyph, .. } => glyph,
        }
    }
}

// ============================================================================
// Output format
// ============================================================================

/// What the renderer should produce.
#[derive(Debug, Clone)]
pub enum OutputFormat {
    /// `tree -L N` style with optional ANSI color.
    Tree {
        /// `None` → unlimited; `Some(n)` → only render down to depth `n`
        /// (root is depth 0; `Some(0)` shows just the root).
        max_depth: Option<usize>,
        /// Apply ANSI colors to status badges. Auto-disable when stdout
        /// isn't a TTY is the caller's responsibility (use `is-terminal`
        /// or `console::Term::is_term`).
        color: bool,
    },
    Json,
    Yaml,
    Toml,
}

impl OutputFormat {
    /// Parse from a CLI flag value: "tree", "json", "yaml", "toml".
    /// `tree` defaults to no depth limit + auto color.
    pub fn parse(s: &str) -> Result<Self, String> {
        match s.to_ascii_lowercase().as_str() {
            "tree" => Ok(OutputFormat::Tree {
                max_depth: None,
                color: true,
            }),
            "json" => Ok(OutputFormat::Json),
            "yaml" | "yml" => Ok(OutputFormat::Yaml),
            "toml" => Ok(OutputFormat::Toml),
            other => Err(format!(
                "unknown format '{other}' (expected: tree, json, yaml, toml)"
            )),
        }
    }
}

// ============================================================================
// Public render entry point
// ============================================================================

/// Render `root` as `format`. The renderer walks `root.children()`
/// recursively — implementors are responsible for cycle prevention
/// (most cascade / dep / dag trees are naturally acyclic).
pub fn render(root: &dyn TreeNode, format: &OutputFormat) -> String {
    match format {
        OutputFormat::Tree { max_depth, color } => render_tree(root, *max_depth, *color),
        OutputFormat::Json => render_json(root),
        OutputFormat::Yaml => render_yaml(root),
        OutputFormat::Toml => render_toml(root),
    }
}

// ============================================================================
// Tree text renderer (tree -L N style)
// ============================================================================

fn render_tree(root: &dyn TreeNode, max_depth: Option<usize>, color: bool) -> String {
    let mut out = String::new();
    render_tree_node(root, "", true, 0, max_depth, color, &mut out, true);
    out
}

/// Continuation prefix for child rows. `│   ` for non-last branches,
/// 4-space pad for last branches. Colored blue alongside the connectors
/// when `color` is on so the structural chrome stays uniform.
fn build_continuation_prefix(prefix: &str, is_root: bool, is_last: bool, color: bool) -> String {
    if is_root {
        String::new()
    } else if is_last {
        format!("{prefix}    ")
    } else if color {
        format!("{prefix}{}", Style::new().blue().apply_to("│   "))
    } else {
        format!("{prefix}│   ")
    }
}

#[allow(clippy::too_many_arguments)]
fn render_tree_node(
    node: &dyn TreeNode,
    prefix: &str,
    is_last: bool,
    depth: usize,
    max_depth: Option<usize>,
    color: bool,
    out: &mut String,
    is_root: bool,
) {
    if let Some(max) = max_depth {
        if depth > max {
            return;
        }
    }

    // Leading connector. nom colors all structural chrome blue
    // regardless of node status — keeps the tree shape readable when
    // most nodes are warning-red mid-failure.
    let connector = if is_root {
        ""
    } else if is_last {
        "└── "
    } else {
        "├── "
    };
    out.push_str(prefix);
    if color && !is_root {
        out.push_str(&Style::new().blue().apply_to(connector).to_string());
    } else {
        out.push_str(connector);
    }

    // Status badge
    if let Some(status) = node.status() {
        let glyph = status.glyph();
        if color {
            out.push_str(&status.style().apply_to(glyph).to_string());
        } else {
            out.push_str(glyph);
        }
        out.push(' ');
    }

    // Label
    out.push_str(&node.label());

    // Inline metadata `[k=v k=v]`
    let meta = node.metadata();
    if !meta.is_empty() {
        out.push_str(" [");
        for (i, (k, v)) in meta.iter().enumerate() {
            if i > 0 {
                out.push(' ');
            }
            out.push_str(k);
            out.push('=');
            out.push_str(v);
        }
        out.push(']');
    }
    out.push('\n');

    // Truncation marker if we're at the depth limit but have hidden children
    let children = node.children();
    if let Some(max) = max_depth {
        if depth >= max && !children.is_empty() {
            let next_prefix = build_continuation_prefix(prefix, is_root, is_last, color);
            out.push_str(&next_prefix);
            out.push_str("... ");
            out.push_str(&format!("({} more)", children.len()));
            out.push('\n');
            return;
        }
    }

    // Children
    let next_prefix = build_continuation_prefix(prefix, is_root, is_last, color);
    let child_count = children.len();
    for (i, child) in children.iter().enumerate() {
        let last = i == child_count - 1;
        render_tree_node(
            *child,
            &next_prefix,
            last,
            depth + 1,
            max_depth,
            color,
            out,
            false,
        );
    }
}

// ============================================================================
// Serializable tree intermediate
// ============================================================================

/// Convert any [`TreeNode`] into a generic value tree. Used by all
/// machine-format renderers — JSON / YAML / TOML serialize this same
/// shape.
fn to_value_tree(node: &dyn TreeNode) -> serde_json::Value {
    use serde_json::{json, Value};
    let mut obj = serde_json::Map::new();
    obj.insert("label".into(), Value::String(node.label()));
    if let Some(status) = node.status() {
        obj.insert("status".into(), Value::String(status.as_str().to_string()));
    }
    let meta = node.metadata();
    if !meta.is_empty() {
        let mut m = serde_json::Map::new();
        for (k, v) in meta {
            m.insert(k, Value::String(v));
        }
        obj.insert("metadata".into(), Value::Object(m));
    }
    let children = node.children();
    if !children.is_empty() {
        obj.insert(
            "children".into(),
            Value::Array(children.iter().map(|c| to_value_tree(*c)).collect()),
        );
    }
    let _ = json!({}); // keeps the `json` import live across feature flags
    Value::Object(obj)
}

// ============================================================================
// Machine-format renderers
// ============================================================================

fn render_json(root: &dyn TreeNode) -> String {
    serde_json::to_string_pretty(&to_value_tree(root))
        .unwrap_or_else(|e| format!("{{\"error\":\"{e}\"}}"))
}

fn render_yaml(root: &dyn TreeNode) -> String {
    serde_yaml::to_string(&to_value_tree(root)).unwrap_or_else(|e| format!("error: {e}"))
}

/// TOML doesn't have an array-rooted document; we wrap the tree under
/// a `tree` key. Serialization goes through serde so nested tables
/// render in proper `[tree.children.0]` table form rather than inline.
fn render_toml(root: &dyn TreeNode) -> String {
    let value = to_value_tree(root);
    let mut wrapper = BTreeMap::new();
    wrapper.insert("tree".to_string(), value);
    match toml::to_string(&wrapper) {
        Ok(t) => t,
        Err(e) => format!("# toml render error: {e}\n"),
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    /// Test fixture: a simple labelled tree with optional status.
    struct N {
        label: String,
        status: Option<NodeStatus>,
        metadata: Vec<(String, String)>,
        kids: Vec<N>,
    }

    impl N {
        fn leaf(label: &str) -> Self {
            Self {
                label: label.into(),
                status: None,
                metadata: vec![],
                kids: vec![],
            }
        }
        fn with_status(mut self, s: NodeStatus) -> Self {
            self.status = Some(s);
            self
        }
        fn with_kids(mut self, kids: Vec<N>) -> Self {
            self.kids = kids;
            self
        }
        fn with_meta(mut self, k: &str, v: &str) -> Self {
            self.metadata.push((k.into(), v.into()));
            self
        }
    }

    impl TreeNode for N {
        fn label(&self) -> String {
            self.label.clone()
        }
        fn status(&self) -> Option<NodeStatus> {
            self.status.clone()
        }
        fn metadata(&self) -> Vec<(String, String)> {
            self.metadata.clone()
        }
        fn children(&self) -> Vec<&dyn TreeNode> {
            self.kids.iter().map(|c| c as &dyn TreeNode).collect()
        }
    }

    fn sample() -> N {
        N::leaf("your-system")
            .with_status(NodeStatus::Ok)
            .with_kids(vec![
                N::leaf("hello-2.12.1")
                    .with_status(NodeStatus::Ok)
                    .with_meta("size", "1.2M")
                    .with_kids(vec![N::leaf("glibc").with_status(NodeStatus::Ok)]),
                N::leaf("bash-5.2")
                    .with_status(NodeStatus::InProgress)
                    .with_kids(vec![N::leaf("ncurses").with_status(NodeStatus::Pending)]),
            ])
    }

    #[test]
    fn tree_format_uses_box_drawing_chars() {
        let out = render(
            &sample(),
            &OutputFormat::Tree {
                max_depth: None,
                color: false,
            },
        );
        assert!(out.contains("├──"), "missing ├──: {out}");
        assert!(out.contains("└──"), "missing └──: {out}");
        assert!(out.contains("│"), "missing │: {out}");
        assert!(out.contains("your-system"), "missing root label: {out}");
        assert!(out.contains("hello-2.12.1"));
        assert!(out.contains("glibc"));
    }

    #[test]
    fn tree_max_depth_truncates_with_marker() {
        let out = render(
            &sample(),
            &OutputFormat::Tree {
                max_depth: Some(1),
                color: false,
            },
        );
        // root + first level visible; deeper truncated
        assert!(out.contains("hello-2.12.1"));
        assert!(out.contains("bash-5.2"));
        // glibc + ncurses are at depth 2 → must be hidden
        assert!(
            !out.contains("glibc"),
            "glibc leaked past depth limit: {out}"
        );
        assert!(!out.contains("ncurses"));
        // truncation marker present
        assert!(out.contains("more)"), "missing truncation marker: {out}");
    }

    #[test]
    fn tree_max_depth_zero_shows_only_root() {
        let out = render(
            &sample(),
            &OutputFormat::Tree {
                max_depth: Some(0),
                color: false,
            },
        );
        assert!(out.contains("your-system"));
        assert!(!out.contains("hello-2.12.1"));
        assert!(out.contains("more)"));
    }

    #[test]
    fn tree_metadata_renders_inline() {
        let out = render(
            &sample(),
            &OutputFormat::Tree {
                max_depth: None,
                color: false,
            },
        );
        assert!(out.contains("[size=1.2M]"), "missing metadata: {out}");
    }

    #[test]
    fn tree_status_glyphs_present() {
        // Glyph set matches nix-output-monitor (nom)/nh exactly:
        //   ✔ done   ⏵ running   ⏸ pending   ⚠ failed
        let out = render(
            &sample(),
            &OutputFormat::Tree {
                max_depth: None,
                color: false,
            },
        );
        assert!(out.contains("✔"), "missing ✔ (Ok): {out}");
        assert!(out.contains("⏵"), "missing ⏵ (InProgress): {out}");
        assert!(out.contains("⏸"), "missing ⏸ (Pending): {out}");
    }

    #[test]
    fn tree_failed_glyph_renders() {
        let n = N::leaf("crashed").with_status(NodeStatus::Failed);
        let out = render(
            &n,
            &OutputFormat::Tree {
                max_depth: None,
                color: false,
            },
        );
        assert!(out.contains("⚠"), "missing ⚠ (Failed): {out}");
    }

    #[test]
    fn json_format_round_trips() {
        let out = render(&sample(), &OutputFormat::Json);
        let parsed: serde_json::Value = serde_json::from_str(&out).expect("valid JSON");
        assert_eq!(parsed["label"], "your-system");
        assert_eq!(parsed["status"], "ok");
        assert!(parsed["children"].is_array());
        assert_eq!(parsed["children"][0]["label"], "hello-2.12.1");
        assert_eq!(parsed["children"][0]["metadata"]["size"], "1.2M");
        assert_eq!(parsed["children"][0]["children"][0]["label"], "glibc");
    }

    #[test]
    fn yaml_format_round_trips() {
        let out = render(&sample(), &OutputFormat::Yaml);
        let parsed: serde_yaml::Value = serde_yaml::from_str(&out).expect("valid YAML");
        assert_eq!(parsed["label"].as_str(), Some("your-system"));
        assert_eq!(parsed["status"].as_str(), Some("ok"));
    }

    #[test]
    fn toml_format_wraps_under_tree_key() {
        let out = render(&sample(), &OutputFormat::Toml);
        let parsed: toml::Value = toml::from_str(&out).expect("valid TOML");
        assert_eq!(parsed["tree"]["label"].as_str(), Some("your-system"));
    }

    #[test]
    fn output_format_parses_cli_strings() {
        assert!(matches!(
            OutputFormat::parse("tree").unwrap(),
            OutputFormat::Tree { .. }
        ));
        assert!(matches!(
            OutputFormat::parse("JSON").unwrap(),
            OutputFormat::Json
        ));
        assert!(matches!(
            OutputFormat::parse("yml").unwrap(),
            OutputFormat::Yaml
        ));
        assert!(matches!(
            OutputFormat::parse("toml").unwrap(),
            OutputFormat::Toml
        ));
        assert!(OutputFormat::parse("xml").is_err());
    }

    #[test]
    fn empty_children_render_correctly_in_all_formats() {
        let lonely = N::leaf("only-me").with_status(NodeStatus::Pending);
        let tree = render(
            &lonely,
            &OutputFormat::Tree {
                max_depth: None,
                color: false,
            },
        );
        assert!(tree.contains("only-me"));
        assert!(tree.contains("⏸"), "Pending should render as ⏸: {tree}");
        let json = render(&lonely, &OutputFormat::Json);
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert!(
            parsed.get("children").is_none(),
            "should omit empty children"
        );
    }

    #[test]
    fn custom_status_glyph_renders() {
        let n = N::leaf("custom").with_status(NodeStatus::Custom {
            glyph: "?".into(),
            color: Some("yellow".into()),
        });
        let out = render(
            &n,
            &OutputFormat::Tree {
                max_depth: None,
                color: false,
            },
        );
        assert!(out.contains("? custom"), "missing custom glyph: {out}");
    }
}
