use std::fmt::Debug;
use std::ops::Deref;
use std::{collections::HashMap, fmt, iter::FromIterator};
extern crate core;
use paste::paste;
extern crate proc_macro;

use validator::{Validate, ValidationError};

use bigdecimal::BigDecimal;
use chrono::{DateTime, Local, Utc};
use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};
use rust_decimal::Decimal;
use serde::Deserialize;
use std::str::FromStr;

use field_names::FieldNames;
use proc_macro::TokenStream;
use rust_decimal_macros::dec;
use std::error::Error;
use tokio_postgres::types::{FromSql, ToSql};
use tokio_postgres::{Client, Config, GenericClient, NoTls, Row};
use uuid::Uuid;

trait Criteria {}

#[derive(Debug)]
pub struct ThingId(String);

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

type Field = String;
type Value = (dyn ToSql + Sync);

enum QueryCondition<'a> {
    Eq(Field, &'a Value),
    Neq(Field, &'a Value),
    Gt(Field, &'a Value),
    Gte(Field, &'a Value),
    Lt(Field, &'a Value),
    Lte(Field, &'a Value),
    In(Field, &'a Value),
    Nin(Field, &'a Value),
    Like(Field, &'a Value),
    NLike(Field, &'a Value),
}

fn query_cond_to_string(q_cond: &QueryCondition, n: i32) -> String {
    match q_cond {
        QueryCondition::Eq(f, _) => format!("{} = ${}", f, n.to_string()),
        QueryCondition::Neq(f, _) => format!("{} != ${}", f, n.to_string()),
        QueryCondition::Gt(f, _) => format!("{} > ${}", f, n.to_string()),
        QueryCondition::Gte(f, _) => format!("{} >= ${}", f, n.to_string()),
        QueryCondition::Lt(f, _) => format!("{} <= ${}", f, n.to_string()),
        QueryCondition::Lte(f, _) => format!("{} <= ${}", f, n.to_string()),
        QueryCondition::In(f, _) => format!("{} = Any(${})", f, n.to_string()),
        QueryCondition::Nin(f, _) => format!("{} != Any(${})", f, n.to_string()),
        QueryCondition::Like(f, _) => format!("{} like ${}", f, n.to_string()),
        QueryCondition::NLike(f, _) => format!("{} not like ${}", f, n.to_string()),
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
                (format!("{} and {}", q, query_cond_to_string(x, i)), i + 1)
            });
        let query_with_where = format!("{} where 1 = 1 {}", base_query, where_part);
        let params = query_conditions
            .into_iter()
            .map(|x| match x {
                QueryCondition::Eq(_, p) => *p,
                QueryCondition::Neq(_, p) => *p,
                QueryCondition::Gt(_, p) => *p,
                QueryCondition::Gte(_, p) => *p,
                QueryCondition::Lt(_, p) => *p,
                QueryCondition::Lte(_, p) => *p,
                QueryCondition::In(_, p) => *p,
                QueryCondition::Nin(_, p) => *p,
                QueryCondition::Like(_, p) => *p,
                QueryCondition::NLike(_, p) => *p,
            })
            .collect();
        (query_with_where, params)
    }
}

async fn select_all<'a, A>(
    client: &Client,
    table: &String,
    query_conditions: &Vec<QueryCondition<'a>>,
    map_row: &(dyn Fn(&Row) -> A),
) -> Result<Vec<A>, Box<dyn Error>> {
    let (query, params) = generate_select(table, query_conditions);
    println!("{}", &query);
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
    (
        $(#[$struct_meta:meta])*
        pub struct $name:ident {
            $(
                $(#[$field_meta:meta])*
                $field_vis:vis $field_name:ident : $field_type:ty
            ),*$(,)+
    }) => {
        $(#[$struct_meta])*
        pub struct $name {
            $(
                $(#[$field_meta])*
                pub $field_name : $field_type,
            )*
        }

        paste! {
            #[derive(Debug)]
            enum [<$name Fields>] {
                $([<$field_name:camel>]),*
            }

            impl fmt::Display for [<$name Fields>] {
                fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                    fmt::Debug::fmt(self, f)
                }
            }
        }

        paste! {
            #[derive(Debug)]
            enum [<$name Criteria>] {
                $([<$field_name:camel Eq>]($field_type)),*,
                $([<$field_name:camel Neq >]($field_type)),*,
                $([<$field_name:camel Gt>]($field_type)),*,
                $([<$field_name:camel Gte>]($field_type)),*,
                $([<$field_name:camel Lt>]($field_type)),*,
                $([<$field_name:camel Lte>]($field_type)),*,
                $([<$field_name:camel In>](Vec<$field_type>)),*,
                $([<$field_name:camel Nin>](Vec<$field_type>)),*,
                $([<$field_name:camel Like>]($field_type)),*,
                $([<$field_name:camel NLike>]($field_type)),*,
            }

            #[derive(Default,Debug)]
            struct [<$name CriteriaStruct>] {
                $([<$field_name _eq>]: Option<$field_type>),*,
                $([<$field_name _neq >]: Option<$field_type>),*,
                $([<$field_name _gt>]: Option<$field_type>),*,
                $([<$field_name _gte>]: Option<$field_type>),*,
                $([<$field_name _lt>]: Option<$field_type>),*,
                $([<$field_name _lte>]: Option<$field_type>),*,
                $([<$field_name _in>]: Vec<$field_type>),*,
                $([<$field_name _nin>]: Vec<$field_type>),*,
                $([<$field_name _like>]: Option<$field_type>),*,
                $([<$field_name _nlike>]: Option<$field_type>),*,
            }

            impl [<$name Criteria>] {
                fn to_query_condition<'a>(&'a self) -> QueryCondition<'a> {
                    match self {
                        $([<$name Criteria>]::[<$field_name:camel Eq>](x) => QueryCondition::Eq(stringify!($field_name).to_string(), x)),*,
                        $([<$name Criteria>]::[<$field_name:camel Neq>](x) => QueryCondition::Neq(stringify!($field_name).to_string(), x)),*,
                        $([<$name Criteria>]::[<$field_name:camel Gt>](x) => QueryCondition::Gt(stringify!($field_name).to_string(), x)),*,
                        $([<$name Criteria>]::[<$field_name:camel Gte>](x) => QueryCondition::Gte(stringify!($field_name).to_string(), x)),*,
                        $([<$name Criteria>]::[<$field_name:camel Lt>](x) => QueryCondition::Lt(stringify!($field_name).to_string(), x)),*,
                        $([<$name Criteria>]::[<$field_name:camel Lte>](x) => QueryCondition::Lte(stringify!($field_name).to_string(), x)),*,
                        $([<$name Criteria>]::[<$field_name:camel In>](x) => QueryCondition::In(stringify!($field_name).to_string(), x)),*,
                        $([<$name Criteria>]::[<$field_name:camel Nin>](x) => QueryCondition::Nin(stringify!($field_name).to_string(), x)),*,
                        $([<$name Criteria>]::[<$field_name:camel Like>](x) => QueryCondition::Like(stringify!($field_name).to_string(), x)),*,
                        $([<$name Criteria>]::[<$field_name:camel NLike>](x) => QueryCondition::NLike(stringify!($field_name).to_string(), x)),*,
                    }
                }
            }

            impl [<$name CriteriaStruct>] {
                fn to_criteria(self) -> Vec<[<$name Criteria>]> {
                    let mut c = vec![];
                    $(if let Some(x) = self.[<$field_name _eq>] {
                        c.push([<$name Criteria>]::[<$field_name:camel Eq>](x));
                    })*
                    $(if let Some(x) = self.[<$field_name _neq>] {
                        c.push([<$name Criteria>]::[<$field_name:camel Neq>](x));
                    })*
                    $(if let Some(x) = self.[<$field_name _gt>] {
                        c.push([<$name Criteria>]::[<$field_name:camel Gt>](x));
                    })*
                    $(if let Some(x) = self.[<$field_name _gte>] {
                        c.push([<$name Criteria>]::[<$field_name:camel Gte>](x));
                    })*
                    $(if let Some(x) = self.[<$field_name _lt>] {
                        c.push([<$name Criteria>]::[<$field_name:camel Lt>](x));
                    })*
                    $(if let Some(x) = self.[<$field_name _lte>] {
                        c.push([<$name Criteria>]::[<$field_name:camel Lte>](x));
                    })*
                    $(if !self.[<$field_name _in>].is_empty() {
                        c.push([<$name Criteria>]::[<$field_name:camel In>](self.[<$field_name _in>]));
                    })*
                    $(if !self.[<$field_name _nin>].is_empty() {
                        c.push([<$name Criteria>]::[<$field_name:camel Nin>](self.[<$field_name _nin>]));
                    })*
                    c
                }
            }
        }


        impl $name {

            fn field_names() -> &'static [&'static str] {
                static NAMES: &'static [&'static str] = &[$(stringify!($field_name)),*];
                NAMES
            }

            fn field_types() -> &'static [&'static str] {
                static TYPES: &'static [&'static str] = &[$(stringify!($field_type)),*];
                TYPES
            }

            fn from_row(row: &Row) -> $name {
                $(let $field_name: $field_type = row.get(stringify!($field_name));)*
                $name {
                    $($field_name),*
                }
           }

            fn to_params_x<'a>(&'a self) -> Vec<&'a (dyn ToSql + Sync)> {
                vec![
                    $(&self.$field_name as &(dyn ToSql + Sync)),*
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

#[derive(Debug, Clone, Copy, Deserialize)]
pub struct UserId {
    id: Uuid,
}

impl ToSql for UserId {
    fn to_sql(
        &self,
        ty: &tokio_postgres::types::Type,
        out: &mut tokio_postgres::types::private::BytesMut,
    ) -> Result<tokio_postgres::types::IsNull, Box<dyn Error + Sync + Send>>
    where
        Self: Sized,
    {
        let UserId { id } = self;
        id.to_sql(ty, out)
    }

    fn accepts(ty: &tokio_postgres::types::Type) -> bool
    where
        Self: Sized,
    {
        <Uuid as ToSql>::accepts(ty)
    }

    fn to_sql_checked(
        &self,
        ty: &tokio_postgres::types::Type,
        out: &mut tokio_postgres::types::private::BytesMut,
    ) -> Result<tokio_postgres::types::IsNull, Box<dyn Error + Sync + Send>> {
        let UserId { id } = self;
        id.to_sql_checked(ty, out)
    }
}

impl<'a> FromSql<'a> for UserId {
    fn from_sql(
        ty: &tokio_postgres::types::Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn Error + Sync + Send>> {
        let uuid = Uuid::from_sql(ty, raw)?;
        Ok(UserId { id: uuid })
    }

    fn from_sql_null(
        ty: &tokio_postgres::types::Type,
    ) -> Result<Self, Box<dyn Error + Sync + Send>> {
        Err(Box::new(tokio_postgres::types::WasNull))
    }

    fn from_sql_nullable(
        ty: &tokio_postgres::types::Type,
        raw: Option<&'a [u8]>,
    ) -> Result<Self, Box<dyn Error + Sync + Send>> {
        match raw {
            Some(raw) => Self::from_sql(ty, raw),
            None => Self::from_sql_null(ty),
        }
    }

    fn accepts(ty: &tokio_postgres::types::Type) -> bool {
        <Uuid as FromSql>::accepts(ty)
    }
}

entity! {
    #[derive(Debug, Deserialize)]
    pub struct User {
        pub id : UserId,
        pub name: String,
        pub money: Decimal,
        pub velocity: Decimal,
        pub start_time: DateTime<Local>,
        pub misc: String,
    }
}

#[derive(Debug, Validate, Deserialize)]
pub struct UserDto {
    pub id: Uuid,
    #[validate(email)]
    pub name: String,
    pub money: Decimal,
    pub velocity: Decimal,
    pub start_time: DateTime<Local>,
    pub misc: String,
}

#[derive(FieldNames)]
pub struct Item {
    pub id: String,
    pub name: String,
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
        id: UserId { id: id },
        name,
        money,
        velocity,
        start_time,
        misc,
    }
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
        id: UserId { id: Uuid::new_v4() },
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
        user.to_params_x().as_slice(),
    )
    .await?;

    /*
    let new_user = User {
        name: "new_email@eml.com".to_string(),
        ..user
    };

    update(
        &client,
        &"demo_sc.users".to_string(),
        &"id".to_string(),
        fields.as_slice(),
        &my_id,
        &new_user.to_params_x(),
    )
    .await?;
    */

    let user_table = &"demo_sc.users".to_string();

    let one = select(
        &client,
        &"demo_sc.users".to_string(),
        &vec![QueryCondition::Eq(
            UserFields::Name.to_string(),
            &"sdkfjls@skdfjls.com".to_string(),
        )],
        &User::from_row,
    )
    .await?;

    let user_crit_struct = UserCriteriaStruct::default();

    let new_crit_struct = UserCriteriaStruct {
        name_eq: Some("sskjdfldsj".to_string()),
        money_eq: Some(dec!(393.0)),
        ..user_crit_struct
    };

    let crit_02 = new_crit_struct.to_criteria();

    // let crit_vec = new_crit_struct.to_criteria();

    // println!("crit vec: {:?}", crit_vec);

    let all = select_all(
        &client,
        user_table,
        &vec![
            UserCriteria::NameIn(vec![
                "bMFObtQQcYAO7CbEvdk8oE62aKZHdW".to_string(),
                "Mg2XJ48FswHl6KENAVsxZXgcUhYDqR".to_string(),
            ])
            .to_query_condition(),
            UserCriteria::MoneyEq(dec!(20000.00)).to_query_condition(),
            // UserCriteria::MiscEq("slfjsdklj".to_string()),
            UserCriteria::VelocityEq(dec!(232.423)).to_query_condition(),
        ],
        &User::from_row,
    )
    .await?;

    for u in all {
        println!("{:?}", u);
    }
    println!("{:?}", one);

    Ok(())
}
