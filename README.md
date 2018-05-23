# Implémentation de réseaux de Kahn 

Implémentation de réseaux de Kahn selon plusieurs archétipes :
Une version repose sur les threads d'Ocaml (kahn.ml)
Une autre utilise les processus Unix pour une implémentation plus efficace (withUnix.ml)
Une troisième implémente en interne la prallélisation (seq.ml)
La dernière permet une implémentation centralisée sur un réseau de machines (rezo.ml)

Ces implémentations ont été réalisées dans le cadre du projet du cours de systèmes de l'ENS Ulm, par Marc Pouzet et Thimothy Bourke.

Utilisation : voir les exemples correspondants pour avoir l'archétype de programmation.
Pour la version en réseau, le client et le serveur doivent avoir le même exécutable, il faut donc passer des arguments en ligne de commande pour faire fonctionner le code. Les arguments possibles sont le choix client/serveur (-c), l'adresse de connexion (--server_adress) pour le client, et le port (-p), optionnel.
