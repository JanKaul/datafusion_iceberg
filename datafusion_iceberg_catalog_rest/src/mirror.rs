use anyhow::anyhow;
use dashmap::DashMap;
use datafusion::{datasource::TableProvider, error::DataFusionError};
use datafusion_iceberg::DataFusionTable;
use futures::{executor::LocalPool, task::LocalSpawnExt};
use std::{collections::HashSet, sync::Arc};

use iceberg_rs::catalog::{identifier::Identifier, namespace::Namespace, Catalog};

type NamespaceNode = HashSet<String>;

enum Node {
    Namespace(NamespaceNode),
    Relation(Arc<dyn TableProvider>),
}

pub struct Mirror {
    storage: DashMap<String, Node>,
    catalog: Arc<dyn Catalog>,
}

impl Mirror {
    pub async fn new(catalog: Arc<dyn Catalog>) -> Result<Self, DataFusionError> {
        let storage = DashMap::new();
        let namespaces = catalog
            .clone()
            .list_namespaces(None)
            .await
            .map_err(|err| DataFusionError::Internal(format!("{}", err)))?;
        for namespace in namespaces {
            let mut namespace_node = HashSet::new();
            let tables = catalog
                .clone()
                .list_tables(&namespace)
                .await
                .map_err(|err| DataFusionError::Internal(format!("{}", err)))?;
            for identifier in tables {
                let relation = catalog
                    .clone()
                    .load_table(&identifier)
                    .await
                    .map_err(|err| DataFusionError::Internal(format!("{}", err)))?;
                namespace_node.insert(identifier.to_string());
                storage.insert(
                    identifier.to_string(),
                    Node::Relation(Arc::new(DataFusionTable::from(relation))),
                );
            }
            storage.insert(namespace.to_string(), Node::Namespace(namespace_node));
        }

        Ok(Mirror { storage, catalog })
    }
    /// Lists all tables in the given namespace.
    pub fn table_names(&self, namespace: &Namespace) -> Result<Vec<Identifier>, DataFusionError> {
        let tables = self
            .storage
            .get(&namespace.to_string())
            .ok_or_else(|| anyhow!("Namespace not found."))
            .map_err(|err| DataFusionError::Internal(format!("{}", err)))?;
        let names = match tables.value() {
            Node::Relation(_) => Err(anyhow!("Cannot list tables of a table.")),
            Node::Namespace(names) => Ok(names),
        }
        .map_err(|err| DataFusionError::Internal(format!("{}", err)))?;
        names
            .iter()
            .map(|x| {
                Identifier::parse(x).map_err(|err| DataFusionError::Internal(format!("{}", err)))
            })
            .collect::<Result<_, DataFusionError>>()
    }
    /// Lists all namespaces in the catalog.
    pub fn schema_names(&self, _parent: Option<&str>) -> Result<Vec<Namespace>, DataFusionError> {
        self.storage
            .iter()
            .filter_map(|r| match r.value() {
                Node::Relation(_) => None,
                Node::Namespace(_) => Some(r.key().clone()),
            })
            .map(|x| {
                Namespace::try_new(
                    x.split(".")
                        .map(|s| s.to_owned())
                        .collect::<Vec<_>>()
                        .as_slice(),
                )
            })
            .collect::<Result<_, anyhow::Error>>()
            .map_err(|err| DataFusionError::Internal(format!("{}", err)))
    }
    pub fn table(&self, identifier: Identifier) -> Option<Arc<dyn TableProvider>> {
        self.storage
            .get(&identifier.to_string())
            .and_then(|x| match x.value() {
                Node::Relation(relation) => Some(relation.clone()),
                Node::Namespace(_) => None,
            })
    }
    pub fn table_exists(&self, identifier: Identifier) -> bool {
        self.storage.contains_key(&identifier.to_string())
    }
    pub fn register_table(
        &self,
        identifier: Identifier,
        table: Arc<dyn TableProvider>,
    ) -> Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        self.storage
            .insert(identifier.to_string(), Node::Relation(table.clone()));
        match self
            .storage
            .get_mut(&identifier.namespace().to_string())
            .ok_or(DataFusionError::Internal(
                "Namespace doesn't exist".to_string(),
            ))?
            .value_mut()
        {
            Node::Namespace(namespace) => {
                namespace.insert(identifier.to_string());
            }
            Node::Relation(_) => {}
        };
        let pool = LocalPool::new();
        let spawner = pool.spawner();
        let cloned_catalog = self.catalog.clone();
        let metadata_location = table
            .clone()
            .as_any()
            .downcast_ref::<DataFusionTable>()
            .ok_or(DataFusionError::Internal(
                "Table is not an iceberg datafusion table.".to_owned(),
            ))?
            .0
            .metadata_location()
            .to_owned();
        spawner
            .spawn_local(async move {
                cloned_catalog
                    .register_table(identifier, &metadata_location)
                    .await
                    .unwrap();
            })
            .map_err(|err| DataFusionError::Internal(format!("{}", err)))?;
        Ok(Some(table))
    }
    pub fn deregister_table(
        &self,
        identifier: Identifier,
    ) -> Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        let table = if let (_, Node::Relation(relation)) = self
            .storage
            .remove(&identifier.to_string())
            .ok_or(DataFusionError::Internal(
                "Can't deregister table, tables doesn't exist.".to_string(),
            ))? {
            Ok(relation)
        } else {
            Err(DataFusionError::Internal(
                "Can't deregister table, identifier refers to a namespace.".to_string(),
            ))
        }?;
        match self
            .storage
            .get_mut(&identifier.namespace().to_string())
            .ok_or(DataFusionError::Internal(
                "Namespace doesn't exist".to_string(),
            ))?
            .value_mut()
        {
            Node::Namespace(namespace) => {
                namespace.remove(&identifier.to_string());
            }
            Node::Relation(_) => {}
        };
        let pool = LocalPool::new();
        let spawner = pool.spawner();
        let cloned_catalog = self.catalog.clone();
        spawner
            .spawn_local(async move {
                cloned_catalog.drop_table(&identifier).await.unwrap();
            })
            .map_err(|err| DataFusionError::Internal(format!("{}", err)))?;
        Ok(Some(table))
    }
}
