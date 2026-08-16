"""
----------------------
Author: Chin Yin Ern |
----------------------

"""
from pyspark.sql import SparkSession
from neo4j import GraphDatabase

class Neo4j:
    driver = None

    @staticmethod
    def create_connection(uri, user, password):
        global driver
        AUTH = (user, password)
        Neo4j.driver = GraphDatabase.driver(uri, auth=AUTH)
        Neo4j.driver.verify_connectivity()
        return Neo4j.driver

    @staticmethod
    def close_connection():
        if Neo4j.driver is not None:
            Neo4j.driver.close()
        
    @staticmethod
    def write_to_neo4j(df, epoch_id, node, relationship=None, properties=None):
        with Neo4j.driver.session() as session:
            for row in df.collect():
                for node_col in node:
                    node_value = row[node_col]
                    # Lambda to create property setting part of the query
                    create_property_query = lambda props: ", ".join([f"n.{prop} = '{row[prop]}'" for prop in props]) if props else ""
                    
                    # If properties exist, create a more detailed query
                    if properties and node_col in properties:
                        property_col = create_property_query(properties[node_col])
                        query = f"MERGE (n:{node_col} {{ {node_col}: '{node_value}' }}) SET {property_col}"
                    else:
                        query = f"MERGE (n:{node_col} {{ {node_col}: '{node_value}' }})"

                    session.run(query)

                if relationship:
                    for start_node, end_node, relationship_type in relationship:
                        if start_node in df.columns and end_node in df.columns:
                            start_node_value = row[start_node]
                            end_node_value = row[end_node]

                            relationship_query = f"""
                                MATCH (a {{{start_node}: $start_node_value}})
                                MERGE (b {{{end_node}: $end_node_value}})
                                MERGE (a)-[:{relationship_type}]->(b)
                            """
                            session.run(relationship_query, start_node_value=start_node_value, end_node_value=end_node_value)


    




