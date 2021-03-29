# Project noSQL

## Prérequis :

Il faut créer un `secret.json` dans le dossier

```
{
    "host":"localhost",
    "user":"root",
    "port":3306,
    "password":"",
    "database":"A4BDD",
    "table": "app",
    "portRedis": 
}
```

## Pour utiliser le projet :

* Il faut que le fichier log soit dans le même dossier

Toutes les requetes demandées dont dans le fichier requests.rest

Pour pouvoir les envoyer, il faut installer l'extension Rest Client dans Visual Studio Code

* Insérer les données en lancant le `server.py`
    * Envoyer la requete `insert`
* Appeler les requetes souhaitées via le `server.py`

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

## SQL

Un fichier SQL avec la table que nous avons utilisée dans notre projet est disponible également sur le GitHub.
