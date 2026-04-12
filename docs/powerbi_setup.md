# ⚙️ Power BI Configuration & Connection Guide

Ce guide explique comment connecter les rapports Power BI au Data Warehouse PostgreSQL (DW) alimenté par le pipeline Airflow + dbt.

## 1. Prérequis
* **Power BI Desktop** installé.
* Accès au serveur PostgreSQL (Host, Port, User, Password).
* Le driver **Npgsql** installé sur votre machine (nécessaire pour la connexion PostgreSQL).

## 2. Paramètres de Connexion
Pour reproduire l'environnement de ce projet, utilisez les paramètres suivants dans Power BI :

* **Serveur :** `votre-serveur-ovh-ou-local` (ex: `localhost` ou IP du serveur)
* **Port :** `5433` (Port standard utilisé dans ce projet)
* **Base de données :** `odoo_dw`
* **Mode de connectivité :** **DirectQuery** > *Note : Le mode DirectQuery est privilégié pour garantir que les visuels reflètent les données dès la fin du run Airflow.*

## 3. Structure du Modèle (Star Schema)
Le rapport s'appuie sur les tables transformées par dbt dans le schéma `analytics` :
* **Faits :** `fact_sale_order`, `fact_crm`, `fact_employee_activity`.
* **Dimensions :** `dim_date`, `dim_vendor`, `dim_product`, `dim_employee`.

## 4. Sécurité (RLS)
Pour le **Cockpit Vendeur**, une sécurité au niveau des lignes (Row-Level Security) est activée :
* La table `dim_vendor` est filtrée par le champ `email` (USERPRINCIPALNAME()) pour que chaque commercial ne puisse voir que ses propres performances.