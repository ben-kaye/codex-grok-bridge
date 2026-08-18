use agent_client_protocol::{ContentBlock, ImageContent, TextContent};
use serde_json::Value;

/// Convert a Codex UserInput array (as JSON) to ACP ContentBlock values.
///
/// Codex UserInput is an enum with variants like `{ "type": "text", "text": "..." }`
/// and `{ "type": "image", "url": "..." }`. We convert these to ACP ContentBlock.
pub fn codex_input_to_acp(inputs: &[Value]) -> Vec<ContentBlock> {
    inputs.iter().filter_map(input_value_to_block).collect()
}

fn input_value_to_block(input: &Value) -> Option<ContentBlock> {
    let ty = input.get("type")?.as_str()?;
    match ty {
        "text" => {
            let text = input.get("text")?.as_str()?;
            Some(ContentBlock::Text(TextContent::new(text)))
        }
        "image" => {
            // Codex image input has a "url" field which may be a data URI or HTTP URL.
            let url = input.get("url")?.as_str()?;
            if let Some(rest) = url.strip_prefix("data:") {
                // Parse data URI: data:<mime>;base64,<data>
                if let Some((mime, data)) = rest.split_once(";base64,") {
                    Some(ContentBlock::Image(
                        ImageContent::new(data, mime).uri(url.to_string()),
                    ))
                } else {
                    // Non-base64 data URI - treat as text fallback
                    Some(ContentBlock::Text(TextContent::new(format!(
                        "[image: {url}]"
                    ))))
                }
            } else {
                // HTTP(S) URL - encode as image with empty data, URI set
                Some(ContentBlock::Image(
                    ImageContent::new("", "image/png").uri(url.to_string()),
                ))
            }
        }
        "localImage" => {
            // LocalImage has a "path" field. We can't inline the data here without reading
            // the file, so represent it as a text placeholder for now.
            let path = input.get("path").and_then(|v| v.as_str()).unwrap_or("");
            Some(ContentBlock::Text(TextContent::new(format!(
                "[local image: {path}]"
            ))))
        }
        _ => {
            // Unknown input type - pass as text description
            Some(ContentBlock::Text(TextContent::new(format!(
                "[unsupported input type: {ty}]"
            ))))
        }
    }
}

/// Convert an ACP ContentBlock to Codex-compatible JSON value.
///
/// This produces a JSON value matching the Codex UserInput or agent message format.
pub fn acp_content_to_codex(block: &ContentBlock) -> Value {
    match block {
        ContentBlock::Text(text) => {
            serde_json::json!({
                "type": "text",
                "text": text.text,
            })
        }
        ContentBlock::Image(img) => {
            // If we have a URI, prefer that; otherwise construct a data URI.
            let url = if let Some(uri) = &img.uri {
                uri.clone()
            } else {
                format!("data:{};base64,{}", img.mime_type, img.data)
            };
            serde_json::json!({
                "type": "image",
                "url": url,
            })
        }
        ContentBlock::Audio(_audio) => {
            serde_json::json!({
                "type": "text",
                "text": "[audio content]",
            })
        }
        ContentBlock::ResourceLink(link) => {
            serde_json::json!({
                "type": "text",
                "text": format!("[resource: {} ({})]", link.name, link.uri),
            })
        }
        ContentBlock::Resource(res) => {
            // Extract text from embedded resource if available
            let text = match &res.resource {
                agent_client_protocol::EmbeddedResourceResource::TextResourceContents(t) => {
                    t.text.clone()
                }
                agent_client_protocol::EmbeddedResourceResource::BlobResourceContents(b) => {
                    format!("[blob resource: {}]", b.uri)
                }
                _ => "[unknown resource type]".to_string(),
            };
            serde_json::json!({
                "type": "text",
                "text": text,
            })
        }
        // Future-proof: handle unknown variants
        _ => {
            serde_json::json!({
                "type": "text",
                "text": "[unknown content type]",
            })
        }
    }
}
