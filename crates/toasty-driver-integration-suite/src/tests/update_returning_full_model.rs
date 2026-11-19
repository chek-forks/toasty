//! A hand-built `Update` must return a complete model row.
//!
//! `Update::<List<M>>::exec` is typed as `Vec<M>`, so the returning clause has
//! to name every column of the model. Projecting only the assigned fields
//! leaves the primary key out and the model cannot be loaded from the partial
//! record — after the write has already been applied.

use crate::prelude::*;
use toasty::stmt::{List, Query, Update};
use toasty_core::stmt::Value;

#[driver_test]
pub async fn hand_built_update_returns_full_record(t: &mut Test) -> Result<()> {
    #[derive(Debug, toasty::Model)]
    struct Item {
        #[key]
        #[auto]
        id: uuid::Uuid,
        name: String,
        age: i64,
    }

    let mut db = t.setup_db(models!(Item)).await;

    let created = toasty::create!(Item {
        name: "alice",
        age: 30
    })
    .exec(&mut db)
    .await?;

    let query = Query::<List<Item>>::all().filter(Item::fields().id().eq(created.id));
    let mut update = Update::<List<Item>>::new(query);
    // Column 1 is `name`. The primary key (column 0) and `age` (column 2) are
    // not assigned, so they can only come back if the whole row is returned.
    update.set(1, Value::from("bob"));

    let items: Vec<Item> = update.exec(&mut db).await?;

    assert_eq!(items.len(), 1);
    assert_eq!(items[0].id, created.id);
    assert_eq!(items[0].name, "bob");
    assert_eq!(items[0].age, 30);

    Ok(())
}
