use crate::prelude::*;

scenario! {
    #[derive(Debug, toasty::Model)]
    struct Document {
        #[key]
        #[auto]
        id: uuid::Uuid,

        body: String,

        revision: i64,
    }

    async fn setup(test: &mut Test) -> toasty::Db {
        test.setup_db(models!(Document)).await
    }
}
