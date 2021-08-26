1. Les données
Vous avez à votre disposition les 4 fichiers de données suivants :
drugs.csv : contient les noms de drugs (des médicaments) avec un id (atccode) et un nom (drug) pubmed.csv : contient des titres d’articles PubMed (title) associés à un journal (journal) à une date donnée (date) ainsi qu’un id (id)
pubmed.json : même structure que pubmed.csv mais en format JSON
clinical_trials.csv : contient des publications scientifiques avec un titre (scientific_title), un id (id), un journal (journal) et une date (date).

2. Le travail à réaliser
L’objectif est de construire une data pipeline permettant de traiter les données définies dans la partie précédente afin de générer le résultat décrit dans la partie 3.
Pour ce faire, vous devez mettre en place un projet en python organisé de la manière qui vous paraît la plus pertinente pour résoudre ce problème. Nous attendons que vous identifiiez une structure de projet et une séparation des étapes nécessaires qui permettent de mettre en évidence vos connaissances autour du développement de jobs data en python.
Il faudra essayer de considérer les hypothèses de travail suivantes :
• Certaines étapes de votre data pipeline pourraient être réutilisées par d’autres data pipelines
• On suppose que votre travail devra être intégré dans un orchestrateur de jobs (de type DAG) par la suite, votre code et la structure choisie doivent donc favoriser cette intégration
• Votre code doit respecter les pratiques que vous mettriez en place dans un cadre professionnel au sein d’une équipe de plusieurs personnes
Nous laissons volontairement un cadre assez libre pour voir votre manière de structurer un projet, de rédiger votre code et de mettre en place les éléments qui vous semblent essentiels dans un projet d’équipe. N’hésitez pas à argumenter votre proposition et les choix que vous faites si nécessaire.

3. Data pipeline
Votre data pipeline doit produire en sortie un fichier JSON qui représente un graphe de liaison entre les différents médicaments et leurs mentions respectives dans les différentes publications PubMed, les différentes publications scientifiques et enfin les journaux avec la date associée à chacune de ces mentions. La représentation ci-dessous permet de visualiser ce qui est attendu. Il peut y avoir plusieurs manières de modéliser cet output et vous pouvez justifier votre vision :
Règles de gestion :
- Un drug est considéré comme mentionné dans un article PubMed ou un essai clinique s’il est mentionné dans le titre de la publication.
- Un drug est considéré comme mentionné par un journal s’il est mentionné dans une publication émise par ce journal.

4. Traitement ad-hoc
Vous devez aussi mettre en place (hors de la data pipeline, vous pouvez considérer que c’est une partie annexe) une feature permettant de répondre à la problématique suivante :
• Extraire depuis le json produit par la data pipeline le nom du journal qui mentionne le plus de médicaments différents

5. Executer le pipeline:

Le command-line pour lancer le pipeline à partir de la root directory du projet:
    
    python -m drug_analysis_pipeline resources/drugs.csv \
        resources/pubmed.csv \
        resources/pubmed.json \
        resources/clinical_trials.csv \
        output
        
le pipe reçoit 5 arguments:
    
    1- drugs csv file path
    2- pubmed csv file path
    3- pubmed json file path
    4- clinical trials csv file path
    5- result directory

6. Tests:
J'ai créé des test sous le repertoire test, et il faut les lancer avec le pytest à partir du root directory

7. La solution Big Data
Quels sont les éléments à considérer pour faire évoluer votre code afin qu’il puisse gérer de grosses volumétries de données (fichiers de plusieurs To ou millions de fichiers par exemple) ?
Pourriez-vous décrire les modifications qu’il faudrait apporter, s’il y en a, pour prendre en considération de telles volumétries ?

La solution est d'utiliser un framework de calcul distribué comme spark ou alternativement dataflow (apache beam). Et afin d'assure que 
le cluster spark a assez de capacité pour traiter des gros quantités des fichier gigantesque, il faut choisir une machine puissante. Aujourd'hui
GCP nous propose le Dataproc afin de lancer telle jobs de spark. 
Pour executer les jobs dataflow, on peut profiter du service (serverless) dataflow de google.


8. Executer le pipeline dans un dag:
Je propose deux methods afin de lancer le pipeline dans un dag d'airflow/
    
    1- packager le module avec la command : python setup.py bdist_wheel, et puis installer le package dist/drug_analysis_pipeline-0.0.1-py3-none-any.whl
    dans l'environment d'airflow, et puis ajouter le python package installer au PYTHONPATH. Et puis on peut tout simplement lancer le pipeline avec 
    l'api PythonOperator ou le BashOperator.
    
    2- Créer une image docker de notre module à partir de fichier Dockerfile, et puis executer le l'image à partir avec le KubernetesPodOperator.  
    
