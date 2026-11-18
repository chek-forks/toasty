use crate::prelude::*;

/// `if_exists` turns an absent key into a zero count, while a plain delete
/// keeps treating it as success. `exec_count` surfaces the count that `exec`
/// discards.
#[driver_test]
pub async fn delete_if_exists_counts_removed_records(t: &mut Test) -> Result<()> {
    #[derive(Debug, toasty::Model)]
    #[table = "exists_items"]
    struct Item {
        #[key]
        id: i64,
        name: String,
    }

    let mut db = t.setup_db(models!(Item)).await;

    Item::create().id(1).name("a").exec(&mut db).await?;

    let removed = Item::filter_by_id(1)
        .delete()
        .if_exists()
        .exec_count(&mut db)
        .await?;
    assert_eq!(removed, 1);

    let removed = Item::filter_by_id(1)
        .delete()
        .if_exists()
        .exec_count(&mut db)
        .await?;
    assert_eq!(removed, 0);

    // Without `if_exists`, deleting a missing key stays an unconditional
    // success on backends that report affected rows.
    Item::filter_by_id(2).delete().exec(&mut db).await?;

    Ok(())
}
