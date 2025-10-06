use clap::{Parser, Subcommand};
use futures::SinkExt as _;
use sqlx::postgres::PgPool;
use std::env;

#[derive(Parser)]
struct Args {
    #[command(subcommand)]
    cmd: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    Add { description: String },
    Done { id: i64 },
}

async fn run() -> anyhow::Result<()> {
    let args = Args::parse_from(wasip3::cli::environment::get_arguments());
    let pool = PgPool::connect(&env::var("DATABASE_URL")?).await?;

    match args.cmd {
        Some(Command::Add { description }) => {
            eprintln!("Adding new todo with description '{description}'");
            let todo_id = add_todo(&pool, description).await?;
            eprintln!("Added new todo with id {todo_id}");
        }
        Some(Command::Done { id }) => {
            eprintln!("Marking todo {id} as done");
            if complete_todo(&pool, id).await? {
                eprintln!("Todo {id} is marked as done");
            } else {
                eprintln!("Invalid id {id}");
            }
        }
        None => {
            eprintln!("Printing list of all todos");
            list_todos(&pool).await?;
        }
    }

    Ok(())
}

async fn add_todo(pool: &PgPool, description: String) -> anyhow::Result<i64> {
    let rec = sqlx::query!(
        r#"
INSERT INTO todos ( description )
VALUES ( $1 )
RETURNING id
        "#,
        description
    )
    .fetch_one(pool)
    .await?;

    Ok(rec.id)
}

async fn complete_todo(pool: &PgPool, id: i64) -> anyhow::Result<bool> {
    let rows_affected = sqlx::query!(
        r#"
UPDATE todos
SET done = TRUE
WHERE id = $1
        "#,
        id
    )
    .execute(pool)
    .await?
    .rows_affected();

    Ok(rows_affected > 0)
}

async fn list_todos(pool: &PgPool) -> anyhow::Result<()> {
    let recs = sqlx::query!(
        r#"
SELECT id, description, done
FROM todos
ORDER BY id
        "#
    )
    .fetch_all(pool)
    .await?;

    for rec in recs {
        eprintln!(
            "- [{}] {}: {}",
            if rec.done { "x" } else { " " },
            rec.id,
            &rec.description,
        );
    }

    Ok(())
}

struct Component;

wasip3::cli::command::export!(Component);

impl wasip3::exports::cli::run::Guest for Component {
    async fn run() -> Result<(), ()> {
        tokio::task::LocalSet::new()
            .run_until(async {
                if let Err(err) = run().await {
                    let (mut tx, rx) = wasip3::wit_stream::new();
                    wasip3::cli::stderr::set_stderr(rx);
                    tx.send(format!("{err:#}\n").into()).await.or(Err(()))?;
                    Err(())
                } else {
                    Ok(())
                }
            })
            .await
    }
}
