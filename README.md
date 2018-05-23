# Implémentation de réseaux de Kahn 

Implémentation de réseaux de Kahn selon plusieurs archétipes :
Une version repose sur les threads d'Ocaml (kahn.ml)
Une autre utilise les processus Unix pour une implémentation plus efficace (withUnix.ml)
Une troisième implémente en interne la prallélisation (seq.ml)
La dernière permet une implémentation centralisée sur un réseau de machines (rezo.ml)

Ces implémentations ont été réalisées dans le cadre du projet du cours de systèmes de l'ENS Ulm, par Marc Pouzet et Thimothy Bourke.
