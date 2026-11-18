use crate::prelude::*;
use toasty_core::driver::Operation;

/// A create builder's `ttl()` reaches the driver as an insert carrying the
/// requested expiry, and the row is still readable afterwards.
#[driver_test(requires(native_ttl))]
pub async fn insert_with_ttl_round_trips(t: &mut Test) -> Result<()> {
    #[derive(Debug, toasty::Model)]
    struct Session {
        #[key]
        id: i64,
        name: String,
    }

    let mut db = t.setup_db(models!(Session)).await;

    t.log().clear();

    Session::create()
        .id(1)
        .name("a")
        .ttl(600)
        .exec(&mut db)
        .await?;

    let session = Session::filter_by_id(1).get(&mut db).await?;
    assert_eq!(session.name, "a");

    let mut ttls = Vec::new();
    while !t.log().is_empty() {
        if let Operation::Insert(op) = t.log().pop_op() {
            ttls.push(op.stmt.as_insert().unwrap().ttl);
        }
    }

    assert!(
        ttls.contains(&Some(600)),
        "expected an insert with ttl=600, saw {ttls:?}"
    );

    Ok(())
}

/// A query update builder's `ttl()` reaches the driver as an update-by-key
/// carrying the requested expiry, and the new value is visible afterwards.
#[driver_test(requires(native_ttl))]
pub async fn update_with_ttl_round_trips(t: &mut Test) -> Result<()> {
    #[derive(Debug, toasty::Model)]
    struct Session {
        #[key]
        id: i64,
        name: String,
    }

    let mut db = t.setup_db(models!(Session)).await;

    Session::create().id(1).name("a").exec(&mut db).await?;

    t.log().clear();

    Session::filter_by_id(1)
        .update()
        .name("b")
        .ttl(600)
        .exec(&mut db)
        .await?;

    let session = Session::filter_by_id(1).get(&mut db).await?;
    assert_eq!(session.name, "b");

    let mut ttls = Vec::new();
    while !t.log().is_empty() {
        if let Operation::UpdateByKey(op) = t.log().pop_op() {
            ttls.push(op.ttl);
        }
    }

    assert!(
        ttls.contains(&Some(600)),
        "expected an update_by_key with ttl=600, saw {ttls:?}"
    );

    Ok(())
}

/// An insert-or-ignore upsert keeps the requested expiry in the upsert
/// operation, and the conflicting row is left unchanged.
#[driver_test(requires(native_ttl))]
pub async fn upsert_ignore_with_ttl(t: &mut Test) -> Result<()> {
    #[derive(Debug, toasty::Model)]
    struct Session {
        #[key]
        id: i64,
        name: String,
    }

    let mut db = t.setup_db(models!(Session)).await;

    Session::create().id(1).name("a").exec(&mut db).await?;

    t.log().clear();

    let ignored = Session::upsert_by_id(1)
        .name("z")
        .ttl(600)
        .or_ignore()
        .exec(&mut db)
        .await?;
    assert!(ignored.is_none());

    let mut ttls = Vec::new();
    while !t.log().is_empty() {
        if let Operation::Upsert(op) = t.log().pop_op() {
            ttls.push(op.stmt.ttl);
        }
    }

    assert!(
        ttls.contains(&Some(600)),
        "expected an upsert with ttl=600, saw {ttls:?}"
    );

    Ok(())
}

/// A TTL on a backend without native support is rejected with an
/// unsupported-feature error instead of being silently dropped.
#[driver_test(requires(not(native_ttl)))]
pub async fn ttl_rejected_without_native_support(t: &mut Test) -> Result<()> {
    #[derive(Debug, toasty::Model)]
    struct Session {
        #[key]
        id: i64,
    }

    let mut db = t.setup_db(models!(Session)).await;

    let result: Result<Session> = Session::create().id(1).ttl(600).exec(&mut db).await;
    let err = result.expect_err("a TTL should be unsupported without native_ttl");

    assert!(
        err.to_string().contains("time-to-live"),
        "unexpected error: {err}"
    );

    Ok(())
}
