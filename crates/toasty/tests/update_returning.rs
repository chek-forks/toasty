//! Regression tests for the returning shape of hand-built updates.
//!
//! `Update::<List<M>>::exec` is typed to return `Vec<M>`, so the statement must
//! return every model column, including the primary key. A `Returning::Changed`
//! statement projects only the assigned fields, which are never key columns, so
//! the model loader rejected the partial record with a type-conversion error
//! *after* the write had already been applied.

use toasty::stmt::{Condition, List, Query, Update};
use toasty_core::stmt::Value;

#[derive(Debug, toasty::Model)]
struct User {
    #[key]
    id: u64,
    name: String,
    age: i64,
}

#[derive(Debug, toasty::Model)]
#[key(partition = region_id, local = mailbox_id)]
struct PartitionedMailbox {
    region_id: String,
    mailbox_id: String,
    status: String,
}

#[derive(Debug, toasty::Model)]
#[key(partition = tenant_id, local = [org_id, id])]
struct ThreeKeyItem {
    tenant_id: String,
    org_id: String,
    id: u64,
    label: String,
}

#[derive(Debug, toasty::Model)]
struct VersionedItem {
    #[key]
    id: u64,
    name: String,
    #[version]
    version: u64,
}

async fn sqlite_db(models: toasty::ModelSet) -> toasty::Db {
    toasty::Db::builder()
        .models(models)
        .build(toasty_driver_sqlite::Sqlite::in_memory())
        .await
        .unwrap()
}

#[tokio::test]
async fn hand_built_update_returns_full_record() {
    let mut db = sqlite_db(toasty::models!(User)).await;
    db.push_schema().await.unwrap();

    toasty::create!(User {
        id: 1u64,
        name: "alice",
        age: 30
    })
    .exec(&mut db)
    .await
    .unwrap();

    let query = Query::<List<User>>::all().filter(User::fields().id().eq(1u64));
    let mut update = Update::<List<User>>::new(query);
    // Column 1 is `name`; the primary key (column 0) is never assigned.
    update.set(1, Value::from("bob"));

    let users: Vec<User> = update.exec(&mut db).await.unwrap();

    assert_eq!(users.len(), 1);
    // The key column and the untouched column both have to survive.
    assert_eq!(users[0].id, 1);
    assert_eq!(users[0].name, "bob");
    assert_eq!(users[0].age, 30);
}

#[tokio::test]
async fn hand_built_update_returns_composite_key() {
    let mut db = sqlite_db(toasty::models!(PartitionedMailbox)).await;
    db.push_schema().await.unwrap();

    toasty::create!(PartitionedMailbox {
        region_id: "r1",
        mailbox_id: "m1",
        status: "active",
    })
    .exec(&mut db)
    .await
    .unwrap();

    let query = Query::<List<PartitionedMailbox>>::all()
        .filter(PartitionedMailbox::fields().region_id().eq("r1"));
    let mut update = Update::<List<PartitionedMailbox>>::new(query);
    update.set(2, Value::from("quarantined"));

    let rows: Vec<PartitionedMailbox> = update.exec(&mut db).await.unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].region_id, "r1");
    assert_eq!(rows[0].mailbox_id, "m1");
    assert_eq!(rows[0].status, "quarantined");
}

#[tokio::test]
async fn hand_built_update_returns_three_part_key() {
    let mut db = sqlite_db(toasty::models!(ThreeKeyItem)).await;
    db.push_schema().await.unwrap();

    toasty::create!(ThreeKeyItem {
        tenant_id: "t1",
        org_id: "o1",
        id: 7u64,
        label: "before",
    })
    .exec(&mut db)
    .await
    .unwrap();

    let query =
        Query::<List<ThreeKeyItem>>::all().filter(ThreeKeyItem::fields().tenant_id().eq("t1"));
    let mut update = Update::<List<ThreeKeyItem>>::new(query);
    update.set(3, Value::from("after"));

    let rows: Vec<ThreeKeyItem> = update.exec(&mut db).await.unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].tenant_id, "t1");
    assert_eq!(rows[0].org_id, "o1");
    assert_eq!(rows[0].id, 7);
    assert_eq!(rows[0].label, "after");
}

#[tokio::test]
async fn hand_built_update_returns_bumped_version() {
    let mut db = sqlite_db(toasty::models!(VersionedItem)).await;
    db.push_schema().await.unwrap();

    toasty::create!(VersionedItem {
        id: 1u64,
        name: "alice"
    })
    .exec(&mut db)
    .await
    .unwrap();

    let query = Query::<List<VersionedItem>>::all().filter(VersionedItem::fields().id().eq(1u64));
    let mut update = Update::<List<VersionedItem>>::new(query);
    update.set(1, Value::from("bob"));

    let rows: Vec<VersionedItem> = update.exec(&mut db).await.unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].id, 1);
    assert_eq!(rows[0].name, "bob");
    // Query updates bump the OCC counter in the engine; the returned record
    // has to carry the value the database now stores.
    assert_eq!(rows[0].version, 2);
}

#[tokio::test]
async fn failed_condition_is_not_a_conversion_error() {
    let mut db = sqlite_db(toasty::models!(User)).await;
    db.push_schema().await.unwrap();

    toasty::create!(User {
        id: 1u64,
        name: "alice",
        age: 30
    })
    .exec(&mut db)
    .await
    .unwrap();

    let query = Query::<List<User>>::all().filter(User::fields().id().eq(1u64));
    let mut update = Update::<List<User>>::new(query);
    update.set(1, Value::from("bob"));
    let update = update.set_condition(Condition::new(User::fields().name().eq("does-not-match")));

    let err = update.exec(&mut db).await.expect_err("condition must fail");
    assert!(
        err.is_condition_failed(),
        "expected a condition failure, got: {err:?}"
    );

    let users = User::filter_by_id(1u64).exec(&mut db).await.unwrap();
    assert_eq!(users[0].name, "alice");
}

/// A statement with no returning clause gets a count back. It must execute and
/// resolve to `()` instead of panicking while collecting rows.
#[tokio::test]
async fn update_without_returning_executes() {
    let mut db = sqlite_db(toasty::models!(User)).await;
    db.push_schema().await.unwrap();

    toasty::create!(User {
        id: 1u64,
        name: "alice",
        age: 30
    })
    .exec(&mut db)
    .await
    .unwrap();

    let query = Query::<List<User>>::all().filter(User::fields().id().eq(1u64));
    let mut update = Update::<List<User>>::new(query);
    update.set(1, Value::from("bob"));
    update.as_untyped_mut().returning = None;

    let stmt: toasty::Statement<()> = toasty::Statement::from_untyped_stmt(update.into_untyped());
    stmt.exec(&mut db).await.unwrap();

    let users = User::filter_by_id(1u64).exec(&mut db).await.unwrap();
    assert_eq!(users[0].name, "bob");
}

/// Generated builders stay on the sparse path: only the assigned fields are
/// returned and merged into the handle.
#[tokio::test]
async fn builder_update_still_reloads_in_place() {
    let mut db = sqlite_db(toasty::models!(User)).await;
    db.push_schema().await.unwrap();

    let mut user = toasty::create!(User {
        id: 1u64,
        name: "alice",
        age: 30
    })
    .exec(&mut db)
    .await
    .unwrap();

    user.update().name("bob").exec(&mut db).await.unwrap();

    assert_eq!(user.id, 1);
    assert_eq!(user.name, "bob");
    assert_eq!(user.age, 30);
}
