/*----------------------
 | order.rs
 | Description: The order document customers upload, with parsing and the validation that decides accept or reject.
 | Author: seanie.gleason
 | Dependencies: serde, serde_json
 | Globals: N/A
 | Params: N/A
 | Returns: N/A
 ----------------------*/

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LineItem {
    pub sku: String,
    pub quantity: u32,
    pub unit_price_cents: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Order {
    pub order_id: String,
    pub customer: String,
    pub items: Vec<LineItem>,
}

impl Order {
    /*----------------------
     | parse
     | Description: Parses and validates an uploaded order; the error is a human-readable rejection reason.
     | Author: seanie.gleason
     | Dependencies: serde_json
     | Globals: N/A
     | Params: bytes -- the uploaded JSON document
     | Returns: the order, or why it was rejected
     ----------------------*/
    pub fn parse(bytes: &[u8]) -> Result<Order, String> {
        let order: Order =
            serde_json::from_slice(bytes).map_err(|e| format!("not an order document: {e}"))?;
        order.validate()?;
        Ok(order)
    }

    /*----------------------
     | validate
     | Description: Rejects orders with no id, no customer, no line items, or a zero quantity.
     | Author: seanie.gleason
     | Dependencies: N/A
     | Globals: N/A
     | Params: N/A
     | Returns: unit, or the rejection reason
     ----------------------*/
    pub fn validate(&self) -> Result<(), String> {
        if self.order_id.trim().is_empty() {
            return Err("order_id is empty".into());
        }
        if self.customer.trim().is_empty() {
            return Err("customer is empty".into());
        }
        if self.items.is_empty() {
            return Err("order has no line items".into());
        }
        if let Some(item) = self.items.iter().find(|item| item.quantity == 0) {
            return Err(format!("line item {} has quantity 0", item.sku));
        }
        Ok(())
    }

    /*----------------------
     | total_cents
     | Description: Sums quantity times unit price over every line item.
     | Author: seanie.gleason
     | Dependencies: N/A
     | Globals: N/A
     | Params: N/A
     | Returns: the order total in cents
     ----------------------*/
    pub fn total_cents(&self) -> u64 {
        self.items
            .iter()
            .map(|item| u64::from(item.quantity) * item.unit_price_cents)
            .sum()
    }
}
