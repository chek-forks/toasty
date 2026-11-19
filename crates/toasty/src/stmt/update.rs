use super::Query;
use crate::{
    Executor, Result,
    schema::{Load, Model},
};
use std::{fmt, marker::PhantomData};
use toasty_core::stmt;

/// A typed update statement.
///
/// `Update` modifies records matching a selection, typically derived from a
/// [`Query`]. Field assignments can be added with [`set`](Update::set) or
/// populated by generated update builders.
///
/// The type parameter `T` is the returning type—it determines what `exec()`
/// produces, not which model is being updated. For example, `Update<User>`
/// returns one updated `User`, while `Update<List<User>>` returns the updated
/// records as `Vec<User>`.
///
/// Generated update-builders wrap this type and expose typed setter methods.
/// You rarely construct `Update` by hand.
pub struct Update<T> {
    pub(crate) untyped: stmt::Update,
    _p: PhantomData<T>,
}

// Methods available on all Update<M> regardless of M
impl<T> Update<T> {
    /// Create a new update statement from the given query selection.
    ///
    /// All records matched by `selection` will be updated. The update returns
    /// the full updated records, which is what [`exec`](Update::exec) promises
    /// through `T::Output`.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[derive(Debug, toasty::Model)]
    /// # struct User {
    /// #     #[key]
    /// #     id: i64,
    /// #     name: String,
    /// # }
    /// use toasty::stmt::{List, Query, Update};
    ///
    /// let update = Update::<List<User>>::new(Query::<List<User>>::all());
    /// ```
    pub fn new(selection: Query<T>) -> Self {
        let mut stmt = selection.untyped.update();
        // `exec` is typed on `T::Output`, so the statement has to return a
        // complete record. Returning only the assigned fields (the sparse
        // `Returning::Changed` projection) omits primary-key columns, and a
        // model cannot be loaded from an incomplete record.
        stmt.returning = Some(stmt::Returning::Model { include: vec![] });

        Self {
            untyped: stmt,
            _p: PhantomData,
        }
    }

    /// Create an update that returns only the assigned fields as a
    /// [`SparseRecord`](toasty_core::stmt::Value::SparseRecord).
    ///
    /// Used by generated update builders, which merge the changed fields into
    /// an instance with [`Load::reload`](crate::Load::reload) instead of
    /// loading a whole model from the response.
    #[doc(hidden)]
    pub fn new_changed(selection: Query<T>) -> Self {
        let mut stmt = selection.untyped.update();
        stmt.returning = Some(stmt::Returning::Changed);

        Self {
            untyped: stmt,
            _p: PhantomData,
        }
    }

    /// Get a mutable reference to the underlying untyped update.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[derive(Debug, toasty::Model)]
    /// # struct User {
    /// #     #[key]
    /// #     id: i64,
    /// #     name: String,
    /// # }
    /// use toasty::stmt::{List, Query, Update};
    ///
    /// let mut update = Update::<List<User>>::new(Query::<List<User>>::all());
    /// let raw = update.as_untyped_mut();
    /// // Inspect or modify the raw update
    /// assert!(raw.returning.is_some());
    /// ```
    pub fn as_untyped_mut(&mut self) -> &mut stmt::Update {
        &mut self.untyped
    }

    /// Replace all field assignments with `assignments`.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[derive(Debug, toasty::Model)]
    /// # struct User {
    /// #     #[key]
    /// #     id: i64,
    /// #     name: String,
    /// # }
    /// use toasty::stmt::{List, Query, Update};
    ///
    /// let mut update = Update::<List<User>>::new(Query::<List<User>>::all());
    /// update.set_assignments(toasty_core::stmt::Assignments::default());
    /// ```
    pub fn set_assignments(&mut self, assignments: stmt::Assignments) {
        self.untyped.assignments = assignments;
    }

    /// Attach a guard condition to this update.
    ///
    /// Unlike the update's filter, a condition does not select which records
    /// to update. It is evaluated after the selection and decides whether the
    /// write is applied. A failed condition surfaces as an error, typically
    /// [`condition_failed`](toasty_core::Error::condition_failed), instead of
    /// silently updating nothing.
    ///
    /// A model with a `#[version]` field already conditions its instance
    /// updates on the version. This method replaces the low-level condition;
    /// the generated update builder's `set_condition` combines a caller
    /// condition with the version guard using `AND`.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[derive(Debug, toasty::Model)]
    /// # struct User {
    /// #     #[key]
    /// #     id: i64,
    /// #     name: String,
    /// # }
    /// use toasty::stmt::{Condition, List, Query, Update};
    ///
    /// let update = Update::<List<User>>::new(Query::<List<User>>::all())
    ///     .set_condition(Condition::new(User::fields().id().eq(1)));
    /// ```
    pub fn set_condition(mut self, condition: stmt::Condition) -> Self {
        self.untyped.condition = condition;
        self
    }

    /// Set a per-write time-to-live in seconds (CQL `USING TTL`).
    ///
    /// Only backends with
    /// [`Capability::native_ttl`](toasty_core::driver::Capability::native_ttl)
    /// accept a TTL; every other backend rejects the statement with
    /// [`unsupported_feature`](toasty_core::Error::unsupported_feature).
    pub fn ttl(mut self, seconds: i64) -> Self {
        self.untyped.ttl = Some(seconds);
        self
    }

    /// Assign a value to a field.
    ///
    /// `field` identifies which field to update and `expr` is the new value.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[derive(Debug, toasty::Model)]
    /// # struct User {
    /// #     #[key]
    /// #     id: i64,
    /// #     name: String,
    /// # }
    /// use toasty::stmt::{List, Query, Update};
    ///
    /// let mut update = Update::<List<User>>::new(Query::<List<User>>::all());
    /// update.set(1, toasty_core::stmt::Value::from("Bob"));
    /// ```
    pub fn set(&mut self, field: impl Into<stmt::Projection>, expr: impl Into<stmt::Expr>) {
        self.untyped.assignments.set(field, expr);
    }

    /// Consume this typed update and return the untyped core statement.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[derive(Debug, toasty::Model)]
    /// # struct User {
    /// #     #[key]
    /// #     id: i64,
    /// #     name: String,
    /// # }
    /// use toasty::stmt::{List, Query, Update};
    ///
    /// let update = Update::<List<User>>::new(Query::<List<User>>::all());
    /// let _raw = update.into_untyped();
    /// ```
    pub fn into_untyped(self) -> stmt::Statement {
        self.untyped.into()
    }
}

impl<T: Load> Update<T> {
    /// Execute this update statement against the given executor.
    ///
    /// # Examples
    ///
    /// ```
    /// # tokio::runtime::Runtime::new().unwrap().block_on(async {
    /// # #[derive(Debug, toasty::Model)]
    /// # struct User {
    /// #     #[key]
    /// #     id: i64,
    /// #     name: String,
    /// # }
    /// # let driver = toasty_driver_sqlite::Sqlite::in_memory();
    /// # let mut db = toasty::Db::builder().models(toasty::models!(User)).build(driver).await.unwrap();
    /// # db.push_schema().await.unwrap();
    /// use toasty::stmt::{List, Query, Update};
    ///
    /// let mut update = Update::<List<User>>::new(Query::<List<User>>::all());
    /// update.set(1, toasty_core::stmt::Value::from("Bob"));
    /// let _users: Vec<User> = update.exec(&mut db).await.unwrap();
    /// # });
    /// ```
    pub async fn exec(self, executor: &mut dyn Executor) -> Result<T::Output> {
        executor.exec(self.into()).await
    }
}

impl<M> Clone for Update<M> {
    fn clone(&self) -> Self {
        Self {
            untyped: self.untyped.clone(),
            _p: PhantomData,
        }
    }
}

impl<M: Model> Default for Update<M> {
    fn default() -> Self {
        Self {
            untyped: stmt::Update {
                target: stmt::UpdateTarget::Model(M::id()),
                assignments: stmt::Assignments::default(),
                filter: stmt::Filter::new(stmt::Expr::from(false)),
                condition: stmt::Condition::default(),
                returning: Some(stmt::Returning::Model { include: vec![] }),
                ttl: None,
            },
            _p: PhantomData,
        }
    }
}

impl<M> AsRef<stmt::Update> for Update<M> {
    fn as_ref(&self) -> &stmt::Update {
        &self.untyped
    }
}

impl<M> AsMut<stmt::Update> for Update<M> {
    fn as_mut(&mut self) -> &mut stmt::Update {
        &mut self.untyped
    }
}

impl<M> fmt::Debug for Update<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.untyped.fmt(f)
    }
}
