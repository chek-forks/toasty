//! cassandra-blog: a small Toasty app shaped for Cassandra and ScyllaDB.
//!
//! The model shows the parts of Toasty that map cleanly onto CQL: UUID keys
//! generated on the client, a composite partition/clustering key, a `Vec<T>`
//! stored as a native CQL list, and a `has_many` / `belongs_to` pair that Toasty
//! loads with ordinary queries (Cassandra has no joins).
//!
//! Run it against the bundled in-memory SQLite database:
//!
//! ```sh
//! cargo run -p example-cassandra-blog
//! ```
//!
//! Or against a local ScyllaDB / Cassandra node:
//!
//! ```sh
//! docker compose up -d scylla
//! cargo run -p example-cassandra-blog --no-default-features --features cassandra
//! ```
//!
//! `TOASTY_CONNECTION_URL` overrides either default, e.g.
//! `cassandra://127.0.0.1:9042/toasty` or `scylla://127.0.0.1:9042/toasty`.

#[derive(Debug, toasty::Model)]
struct Author {
    #[key]
    #[auto]
    id: uuid::Uuid,

    name: String,

    bio: Option<String>,

    #[has_many]
    posts: toasty::Deferred<Vec<Post>>,
}

#[derive(Debug, toasty::Model)]
#[key(partition = author_id, local = id)]
struct Post {
    #[auto]
    id: uuid::Uuid,

    author_id: uuid::Uuid,

    title: String,

    body: String,

    tags: Vec<String>,

    #[belongs_to(key = author_id, references = id)]
    author: toasty::Deferred<Author>,
}

#[tokio::main]
async fn main() -> toasty::Result<()> {
    let default_url = if cfg!(feature = "cassandra") && !cfg!(feature = "sqlite") {
        "cassandra://127.0.0.1:9042/toasty"
    } else {
        "sqlite::memory:"
    };
    let url = std::env::var("TOASTY_CONNECTION_URL").unwrap_or_else(|_| default_url.to_string());

    let mut db = toasty::Db::builder()
        .models(toasty::models!(crate::*))
        .connect(&url)
        .await?;

    db.push_schema().await?;
    println!("connected to {url}; schema created\n");

    let author = toasty::create!(Author {
        name: "Ada",
        bio: Some("writes about distributed systems".to_string()),
    })
    .exec(&mut db)
    .await?;

    assert!(!author.id.is_nil());
    println!("created author {} ({})", author.name, author.id);

    // Both posts share the same partition key (`author_id`), so this write is
    // a single-partition insert — the shape Cassandra is built for.
    let mut first = toasty::create!(in author.posts() {
        title: "Partition keys",
        body: "How CQL spreads rows across a cluster.",
        tags: ["cassandra", "partition-key"],
    })
    .exec(&mut db)
    .await?;

    let second = toasty::create!(in author.posts() {
        title: "Lists are native",
        body: "Vec<String> becomes list<text>.",
        tags: ["cassandra", "collections"],
    })
    .exec(&mut db)
    .await?;

    assert_eq!(first.author_id, author.id);
    assert_eq!(second.author_id, author.id);
    println!("created two posts in partition {}\n", author.id);

    let posts = Post::filter_by_author_id(author.id).exec(&mut db).await?;
    assert_eq!(posts.len(), 2);
    println!(
        "partition query returned {} posts: {:?}",
        posts.len(),
        posts
            .iter()
            .map(|post| post.title.as_str())
            .collect::<Vec<_>>()
    );

    // A full primary-key lookup: partition key + clustering key.
    let loaded = Post::get_by_author_id_and_id(&mut db, &author.id, first.id).await?;
    assert_eq!(loaded.title, "Partition keys");
    assert_eq!(loaded.tags, vec!["cassandra", "partition-key"]);
    println!("point read: {} -> {:?}\n", loaded.title, loaded.tags);

    // `push` lowers to a list append (`tags = tags + [...]`).
    toasty::update!(first { tags.push("cql") })
        .exec(&mut db)
        .await?;

    let reloaded = Post::get_by_author_id_and_id(&mut db, &author.id, first.id).await?;
    assert_eq!(reloaded.tags.len(), 3);
    assert_eq!(reloaded.tags.last().unwrap(), "cql");
    println!("list append: {:?}\n", reloaded.tags);

    toasty::update!(first {
        title: "Partition keys, revisited"
    })
    .exec(&mut db)
    .await?;

    let reloaded = Post::get_by_author_id_and_id(&mut db, &author.id, first.id).await?;
    assert_eq!(reloaded.title, "Partition keys, revisited");
    println!("scalar update: {}\n", reloaded.title);

    first.delete().exec(&mut db).await?;

    let posts = Post::filter_by_author_id(author.id).exec(&mut db).await?;
    assert_eq!(posts.len(), 1);
    let remaining = posts.into_iter().next().unwrap();
    assert_eq!(remaining.id, second.id);
    println!("deleted one post; partition now has 1 left");

    remaining.delete().exec(&mut db).await?;
    author.delete().exec(&mut db).await?;
    println!("\ndone: one partition, two posts, a list append, an update, and a delete");

    Ok(())
}
