use crate::prelude::*;

#[driver_test]
pub async fn partition_only_key_round_trip(t: &mut Test) -> Result<()> {
    #[derive(Debug, toasty::Model)]
    #[table = "things"]
    #[key(partition = [owner_scope, owner_id])]
    struct Thing {
        owner_scope: String,
        owner_id: i64,
        value: i64,
    }

    let mut db = t.setup_db(models!(Thing)).await;

    Thing::create()
        .owner_scope("a")
        .owner_id(1)
        .value(10)
        .exec(&mut db)
        .await?;
    Thing::create()
        .owner_scope("a")
        .owner_id(2)
        .value(20)
        .exec(&mut db)
        .await?;

    let found = Thing::filter_by_owner_scope_and_owner_id("a", 1)
        .get(&mut db)
        .await?;
    assert_eq!(found.value, 10);

    let mut things: Vec<Thing> = Thing::filter(Thing::fields().owner_scope().eq("a".to_string()))
        .exec(&mut db)
        .await?;
    things.sort_by_key(|thing| thing.owner_id);
    assert_eq!(things.len(), 2);
    assert_eq!(things[0].value, 10);
    assert_eq!(things[1].value, 20);

    Thing::filter_by_owner_scope_and_owner_id("a", 1)
        .delete()
        .exec(&mut db)
        .await?;

    assert!(
        Thing::filter_by_owner_scope_and_owner_id("a", 1)
            .get(&mut db)
            .await
            .is_err()
    );

    let remaining = Thing::filter_by_owner_scope_and_owner_id("a", 2)
        .get(&mut db)
        .await?;
    assert_eq!(remaining.value, 20);

    Ok(())
}
