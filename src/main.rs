use std::fmt::Debug;
use std::ops::Deref;
use std::{collections::HashMap, fmt, iter::FromIterator};
extern crate core;
use paste::paste;
extern crate proc_macro;

use bigdecimal::BigDecimal;
use chrono::{DateTime, Local, Utc};
use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};
use rust_decimal::Decimal;
use std::str::FromStr;

use field_names::FieldNames;
use proc_macro::TokenStream;
use rust_decimal_macros::dec;
use std::error::Error;
use tokio_postgres::types::ToSql;
use tokio_postgres::{Client, Config, GenericClient, NoTls, Row};
use uuid::Uuid;

trait Criteria {}

#[derive(Debug)]
pub struct ThingId(String);

macro_rules! enum_str {
    (enum $name:ident {
        $($variant:ident = $val:expr),*,
    }) => {
        enum $name {
            $($variant = $val),*
        }

        impl $name {
            fn name(&self) -> &'static str {
                match self {
                    $($name::$variant => stringify!($variant)),*
                }
            }
        }
    };
}

macro_rules! entity {
    (pub struct $name:ident {
        $(pub $fname:ident : $field_type:ty,)*
    }) => {
        #[derive(Debug)]
        struct $name {
            $($fname : $field_type),*
        }

        paste! {
            #[derive(Debug)]
            enum [<$name Fields>] {
                $([<$fname:camel>]),*
            }

            impl fmt::Display for [<$name Fields>] {
                fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                    fmt::Debug::fmt(self, f)
                }
            }
        }

        paste! {
            enum [<$name Criteria>] {
                $([<$fname:camel Eq>]($field_type)),*,
                $([<$fname:camel Neq >]($field_type)),*,
                $([<$fname:camel Gt>]($field_type)),*,
                $([<$fname:camel Gte>]($field_type)),*,
                $([<$fname:camel Lt>]($field_type)),*,
                $([<$fname:camel Lte>]($field_type)),*,
                $([<$fname:camel In>](Vec<$field_type>)),*,
                $([<$fname:camel Nin>](Vec<$field_type>)),*,
            }

            #[derive(Default,Debug)]
            struct [<$name CriteriaStruct>] {
                $([<$fname _eq>]: Option<$field_type>),*,
                $([<$fname _neq >]: Option<$field_type>),*,
                $([<$fname _gt>]: Option<$field_type>),*,
                $([<$fname _gte>]: Option<$field_type>),*,
                $([<$fname _lt>]: Option<$field_type>),*,
                $([<$fname _lte>]: Option<$field_type>),*,
                $([<$fname _in>]: Vec<$field_type>),*,
                $([<$fname _nin>]: Vec<$field_type>),*,
                search_term: Option<String>,
            }
        }


        impl $name {

            fn field_names() -> &'static [&'static str] {
                static NAMES: &'static [&'static str] = &[$(stringify!($fname)),*];
                NAMES
            }

            fn field_types() -> &'static [&'static str] {
                static TYPES: &'static [&'static str] = &[$(stringify!($field_type)),*];
                TYPES
            }

            fn to_params_x<'a>(&'a self) -> Vec<&'a (dyn ToSql + Sync)> {
                vec![
                    $(&self.$fname as &(dyn ToSql + Sync)),*
                ][1..].into_iter().map(|x| *x as &(dyn ToSql + Sync)).collect::<Vec<&'a (dyn ToSql + Sync)>>()
            }
        }
    }
}

/*
entity! {
pub struct Thing {
    pub id: ThingId,
    pub thing_name: String,
    pub amount: f32,
}
}
*/

entity! {
    pub struct User {
        pub id: Uuid,
        pub name: String,
        pub money: Decimal,
        pub velocity: Decimal,
        pub start_time: DateTime<Local>,
        pub misc: String,
    }
}

fn check_crit() {
    let x = UserCriteria::NameEq("blajlkjd".to_string());
}

#[derive(FieldNames)]
pub struct Item {
    pub id: String,
    pub name: String,
}

fn create_insert_sql(table: &String, id_field: &String, fields: &[String]) -> String {
    let fields_sql: String = fields
        .iter()
        .fold(id_field.clone(), |acc, x| format!("{}, {}", acc, x));
    let field_range = 2..fields.len() + 2;
    let field_params = field_range.fold("$1".to_string(), |acc, x| format!("{}, ${}", acc, x));
    format!(
        "insert into {} ({}) values ({})",
        table, fields_sql, field_params
    )
}

fn create_update_sql(table: &String, id_field: &String, fields: &[String]) -> String {
    let (head, tail) = fields.split_at(1);
    let first = format!("{} = $1", head.first().unwrap());
    let (fields_sql, _) = tail.into_iter().fold((first, 2), |acc, x| {
        let (q, i) = acc;
        (format!("{} , {} = ${}", q, x, i.to_string()), i + 1)
    });
    format!(
        "update {} set {} where {} = ${}",
        table,
        fields_sql,
        id_field,
        fields.len() + 1
    )
}

async fn insert(
    client: &Client,
    table: &String,
    id_field: &String,
    fields: &[String],
    id_param: &(dyn ToSql + Sync),
    params: &[&(dyn ToSql + Sync)],
) -> Result<(), Box<dyn Error>> {
    let insert_sql = create_insert_sql(table, id_field, fields);
    let stmt = client.prepare(&insert_sql).await?;
    let all_params = &[&[id_param], params].concat();
    client.execute(&stmt, all_params.as_slice()).await?;
    Ok(())
}

async fn update(
    client: &Client,
    table: &String,
    id_field: &String,
    fields: &[String],
    id_param: &(dyn ToSql + Sync),
    params: &[&(dyn ToSql + Sync)],
) -> Result<(), Box<dyn Error>> {
    let update_sql = create_update_sql(table, id_field, fields);
    let stmt = client.prepare(&update_sql).await?;
    let all_params = &[params, &[id_param]].concat();
    client.execute(&stmt, all_params.as_slice()).await?;
    Ok(())
}

trait MyT {}

fn mk_params<'a>(user: &'a User) -> [&'a (dyn ToSql + Sync); 5] {
    [
        &user.name,
        &user.money,
        &user.velocity,
        &user.start_time,
        &user.misc,
    ]
}

fn from_row(row: &Row) -> User {
    let id: Uuid = row.get(0);
    let name: String = row.get(1);
    let money: Decimal = row.get(2);
    let velocity: Decimal = row.get(3);
    let start_time: DateTime<Local> = row.get(4);
    let misc: String = row.get(5);
    User {
        id,
        name,
        money,
        velocity,
        start_time,
        misc,
    }
}

const BLAH: &'static str = "hello";

macro_rules! sql_ent {
    (type $name:ident = $other:ident;) => {
        type $name = $other;

        impl $name {
            fn to_params() -> &'static [&'static (dyn ToSql + Sync)] {
                &[]
            }
        }
    };
}

sql_ent! {
    type URow = User;
}

impl URow {
    fn params(&self) -> &[&'static str] {
        URow::field_names()
    }
}

type Field = String;
type Value = (dyn ToSql + Sync);

enum QueryCondition<'a> {
    Eq(&'a Field, &'a Value),
    NEq(&'a Field, &'a Value),
    Gt(&'a Field, &'a Value),
    Gte(&'a Field, &'a Value),
    Lt(&'a Field, &'a Value),
    Lte(&'a Field, &'a Value),
    In(&'a Field, &'a Value),
    Nin(&'a Field, &'a Value),
}

fn queryCondToQueryStr(qCond: &QueryCondition, n: i32) -> String {
    match qCond {
        QueryCondition::Eq(f, _) => format!("{} = ${}", f, n.to_string()),
        QueryCondition::NEq(f, _) => format!("{} != ${}", f, n.to_string()),
        QueryCondition::Gt(f, _) => format!("{} > ${}", f, n.to_string()),
        QueryCondition::Gte(f, _) => format!("{} >= ${}", f, n.to_string()),
        QueryCondition::Lt(f, _) => format!("{} <= ${}", f, n.to_string()),
        QueryCondition::Lte(f, _) => format!("{} <= ${}", f, n.to_string()),
        QueryCondition::In(f, _) => format!("{} in ${}", f, n.to_string()),
        QueryCondition::Nin(f, _) => format!("{} not in ${}", f, n.to_string()),
    }
}

fn generate_select<'a>(
    table: &String,
    query_conditions: &'a Vec<QueryCondition<'a>>,
) -> (String, Vec<&'a (dyn ToSql + Sync)>) {
    let base_query = format!("select * from {}", table);
    if query_conditions.is_empty() {
        (base_query, vec![])
    } else {
        let (where_part, _) = query_conditions
            .into_iter()
            .fold(("".to_string(), 1), |acc, x| {
                let (q, i) = acc;
                (format!("{} and {}", q, queryCondToQueryStr(&x, i)), i + 1)
            });
        let query_with_where = format!("{} where 1 = 1 {}", base_query, where_part);
        let params = query_conditions
            .into_iter()
            .map(|x| match x {
                QueryCondition::Eq(_, p) => *p,
                QueryCondition::NEq(_, p) => *p,
                QueryCondition::Gt(_, p) => *p,
                QueryCondition::Gte(_, p) => *p,
                QueryCondition::Lt(_, p) => *p,
                QueryCondition::Lte(_, p) => *p,
                QueryCondition::In(_, p) => *p,
                QueryCondition::Nin(_, p) => *p,
            })
            .collect();
        (query_with_where, params)
    }
}

/*
fn blah(ps: Vec<&(dyn ToSql + Sync)>) -> &[&(dyn ToSql + Sync)] {
    ps.into_iter()
        .map(|x| x)
        .collect::<Vec<&(dyn ToSql + Sync)>>()
        .as_slice()
}
*/

async fn select_all<'a, A>(
    client: &Client,
    table: &String,
    query_conditions: &'a Vec<QueryCondition<'a>>,
    map_row: &(dyn Fn(&Row) -> A),
) -> Result<Vec<A>, Box<dyn Error>> {
    let (query, params) = generate_select(table, query_conditions);
    let stmt = client.prepare(&query).await?;
    let row = client.query(&stmt, params.as_slice()).await?;
    Ok(row.iter().map(map_row).collect())
}

async fn select<'a, A>(
    client: &Client,
    table: &String,
    query_conditions: &'a Vec<QueryCondition<'a>>,
    map_row: &(dyn Fn(&Row) -> A),
) -> Result<Option<A>, Box<dyn Error>> {
    let (query, params) = generate_select(table, query_conditions);
    let stmt = client.prepare(&query).await?;
    let row = client.query_opt(&stmt, params.as_slice()).await?;
    Ok(row.map(move |x| map_row(&x)))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    /*
    let _thing = Thing {
        id: ThingId("23243".to_string()),
        thing_name: "Name".to_string(),
        amount: 23.43,
    };
    println!("{:?}", Thing::field_names());
    println!("{:?}", Thing::field_types());
    println!("{:?}", Item::FIELDS);
    */

    let rand_string: String = thread_rng()
        .sample_iter(&Alphanumeric)
        .take(30)
        .map(char::from)
        .collect();

    println!("{:?}", User::field_names());
    println!("{:?}", User::field_types());

    let user = User {
        id: Uuid::new_v4(),
        name: rand_string,
        money: dec!(20000.00),
        velocity: dec!(23.23),
        start_time: Local::now(),
        misc: "this is a note".to_string(),
    };

    let (client, conn) = Config::new()
        .host("localhost")
        .port(5558)
        .user("postgres")
        .password("postgres")
        .dbname("postgres")
        .connect(NoTls)
        .await?;

    tokio::spawn(async move {
        if let Err(e) = conn.await {
            eprintln!("connection error: {}", e)
        }
    });

    let my_id = Uuid::parse_str("29f20475-357d-442a-a51c-f71a2d88cd10")?;

    let rows = client
        .query("select * from demo_sc.users where id = $1", &[&my_id])
        .await?;

    println!("{}", rows.len());

    for r in rows {
        let x: String = r.get("name");
        println!("{}", x);
    }

    let fields: Vec<String> = User::field_names()
        .iter()
        .map(|x| x.to_string())
        .filter(|x| x != &"id".to_string())
        .collect();

    let params: &[&(dyn ToSql + Sync)] = &[
        &user.name,
        &user.money,
        &user.velocity,
        &user.start_time,
        &user.misc,
    ];

    let params_foo: Vec<&(dyn ToSql + Sync)> = vec![
        &user.name as &(dyn ToSql + Sync),
        &user.money as &(dyn ToSql + Sync),
        &user.velocity as &(dyn ToSql + Sync),
        &user.start_time as &(dyn ToSql + Sync),
        &user.misc as &(dyn ToSql + Sync),
    ][..1]
        .into_iter()
        .map(|x| *x as &(dyn ToSql + Sync))
        .collect::<Vec<&(dyn ToSql + Sync)>>();

    println!("Pringing user params");
    println!("{:?}", user.to_params_x());

    insert(
        &client,
        &"demo_sc.users".to_string(),
        &"id".to_string(),
        fields.as_slice(),
        &user.id,
        // params,
        user.to_params_x().as_slice(),
    )
    .await?;

    let new_user = User {
        name: "new_email@eml.com".to_string(),
        ..user
    };

    let new_params = mk_params(&new_user);

    /*
    update(
        &client,
        &"demo_sc.users".to_string(),
        &"id".to_string(),
        fields.as_slice(),
        &my_id,
        &new_params,
    )
    .await?;
    */

    let all = select_all(
        &client,
        &"demo_sc.users".to_string(),
        &vec![QueryCondition::Eq(
            &"name".to_string(),
            &"sdkfjls@skdfjls.com".to_string(),
        )],
        &from_row,
    )
    .await?;

    let user_table = &"demo_sc.users".to_string();

    let one = select(
        &client,
        &"demo_sc.users".to_string(),
        &vec![QueryCondition::Eq(
            &UserFields::Name.to_string(),
            &"sdkfjls@skdfjls.com".to_string(),
        )],
        &from_row,
    )
    .await?;

    let userCritStruct = UserCriteriaStruct::default();

    println!("user crite struct: {:?}", userCritStruct);

    let all = select_all(
        &client,
        user_table,
        &vec![QueryCondition::Eq(
            &UserFields::Money.to_string(),
            &dec!(20000.0000),
        )],
        &from_row,
    )
    .await?;

    for u in all {
        println!("{:?}", u);
    }
    println!("{:?}", one);

    Ok(())
}
