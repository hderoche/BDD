# Project noSQL

## Installer les dépendances

```
pip install -r requirements.txt
``` 

## Raw data -> SQL

Afin d'importer toutes les données dans 
le projet, un serveur python permet de faire des requetes API [POST, GET, ...] pour inserer les documents dans une base de donnéees relationelle.

## SQL -> MongoDB

A partir des données raw contenue dans la table SQL, nous les traitons pour répondre aux différentes requetes que l'utilisateur final pourra faire.

* Récupérer le cycle de vie parcouru (la liste des status d’un objet donné)
* Compter le nombre d’objets par status
* Compter le nombre d’objets par status sur la dernière heure
* Compter le nombre d’objets respectant l’intégrité du graphe du cycle de vie

Pour ce faire, nous avons implémenté un module qui s'occupe de faire les stats au fur et à mesure que les fichiers sont ajoutés dans la base de donnée MongoDB

Seule la requete concernant le nombre d'objets par status sur la dernière heure sera faire en temps réel, puisque nous avons besoin du timestamp de la requete.

## SQL -> Redis

La meme approche est effectuée côté Redis.