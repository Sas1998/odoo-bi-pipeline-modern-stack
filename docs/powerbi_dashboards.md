# 📊 Power BI — Business Intelligence Layer

> Les dashboards Power BI constituent la couche de consommation finale du pipeline ELT. Chacun est connecté directement au Data Warehouse PostgreSQL via DirectQuery, et expose une logique métier encapsulée dans des mesures DAX. Les données sources proviennent d'Odoo via le pipeline Airflow + dbt décrit dans ce dépôt.

---

## 1. Dashboard — Direction Commerciale

![Direction Commerciale — Analyse des Opportunités et du CA](docs/screenshots/Direction_Commercial1.png)

### Objectif Business

Fournir à la direction commerciale une vue temps réel de l'entonnoir de ventes complet — du volume d'opportunités CRM jusqu'au chiffre d'affaires RBE encaissé — avec un suivi mois-sur-mois automatique ne nécessitant aucune maintenance manuelle.

### Pages incluses

| Page | Contenu |
|------|---------|
| **Opportunités** | Volume total, taux de gain/perte, évolution mensuelle par étape CRM |
| **Signé** | CA TTC signé et encaissé, comparatif M vs M-1 par agence et type de projet |
| **Annulé** | Taux d'annulation, motifs de perte, répartition par agence et produit |
| **Portefeuille des ventes** | Détail ligne à ligne par vendeur, client, statut et date de signature |

### Analyse de l'Expertise DAX

#### Mesure clé 1 — `Opportunity MoM% Dynamic` : Time Intelligence sans calendrier standard

```dax
VAR _LatestDate =
    MAXX(ALL(fact_crm), RELATED(dim_date[date]))

VAR _CurrentPeriod =
    CALCULATE(
        MAX(dim_date[CustomMonthPeriod]),
        FILTER(ALL(dim_date), dim_date[date] = _LatestDate)
    )

VAR _PreviousPeriod =
    CALCULATE(
        MAX(dim_date[CustomMonthPeriod]),
        FILTER(ALL(dim_date), dim_date[CustomMonthPeriod] < _CurrentPeriod)
    )
```

**Pourquoi c'est complexe :** Odoo utilise un cycle de reporting décalé (clôture le 25 du mois, pas le dernier jour du calendrier). Plutôt que d'utiliser `DATEADD` ou `PREVIOUSMONTH` — qui s'appuient sur un calendrier continu — la mesure résout dynamiquement la période courante et la précédente à partir de la *dernière date de mise à jour d'état réelle dans les faits*. Cela garantit que le comparatif M-1 est toujours cohérent avec la logique métier, indépendamment de la date d'actualisation du dataset. L'utilisation de `TREATAS` pour mapper les périodes sur une dimension de date non liée physiquement est une technique avancée de gestion du contexte de filtre.

#### Mesure clé 2 — `% of Total Opportunities` par étape avec `ALL(dim_crm_stage)`

```dax
MEASURE 'fact_crm'[% of Total Opportunities] =
DIVIDE(
    CALCULATE(COUNTROWS(fact_crm), dim_type_crm[type] = "opportunity"),
    CALCULATE([Total Opportunities], ALL(dim_crm_stage))
)
```

**Pourquoi c'est complexe :** Le `ALL(dim_crm_stage)` supprime explicitement le filtre de dimension d'étape tout en préservant les autres filtres actifs (vendeur, société, période). Sans cette instruction, le dénominateur serait filtré par l'étape sélectionnée, rendant le calcul du pourcentage incorrect. Cela permet à chaque barre du graphique de répartition par étape d'afficher sa part du total global.

### Points Forts Visuels

- **KPI cards dynamiques** : Chaque indicateur principal (Total Opportunities, Opportunités Gagnées, Perdues) affiche simultanément la valeur absolue, la moyenne mensuelle calculée sur l'historique, la variation M-1 en % avec indicateur directionnel (▲/▼), et un sous-titre contextuel du type *"MoM% | mai 2025 vs avril 2025"* — entièrement auto-généré en DAX.
- **Graphique de répartition par étape** : Visualisation en barres horizontales à 100% montrant la part de chaque étape CRM (Visite technique planifiée, Nouveau, Perdu, BDC consolidé…) sur le total des opportunités.
- **Évolution mensuelle** : Courbe de tendance des opportunités avec annotation automatique des valeurs min/max uniquement (implémentée via `OpportunityLabel` qui retourne `BLANK()` pour les valeurs intermédiaires, évitant ainsi la surcharge visuelle).
- **Matrice de comparaison M-1** : Tableau croisé par type de projet affichant CA M-1, CA courant, et % comparatif coloré conditionnellement (vert/rouge).
- **Analyse des annulations** : Graphique en barres empilées à 100% par agence et par type de produit (PV, PAC, Bt…), permettant d'identifier les agences à fort taux d'annulation sur certains produits.

---

## 2. Dashboard — Cockpit Vendeur

![Cockpit Vendeur — Suivi Personnel des Objectifs](docs/screenshots/cockpit_vendeur.jpg)

### Objectif Business

Offrir à chaque commercial un cockpit individuel lui permettant de suivre en temps réel sa progression vers son objectif annuel, avec un ajustement automatique de la cible selon sa date d'embauche — pour ne jamais pénaliser un vendeur recruté en cours d'année.

### Pages incluses

| Page | Contenu |
|------|---------|
| **Récapitulatif** | Jauge d'objectif annuel ajusté, KPIs RBE, taux de gain/perte, classement |
| **CA Mensuel** | Comparatif M-1, objectif mensuel, détail par type de projet et paiement |

### Analyse de l'Expertise DAX

#### Mesure clé 1 — `Adjusted_Annual_Target` : Proratisation de l'objectif selon l'ancienneté

```dax
MEASURE 'fact_sale_order'[Adjusted_Annual_Target] =
VAR BeginDate = MIN(dim_vendor[BeginDate])
VAR MonthsActive =
    IF(YEAR(BeginDate) < 2025, 12, 13 - MONTH(BeginDate))
RETURN
    DIVIDE(MonthsActive, 12) * 4000000
```

**Pourquoi c'est complexe :** Un objectif annuel fixe de 4 MCHF appliqué à un vendeur recruté en septembre serait injuste et inutilisable. Cette mesure calcule dynamiquement le nombre de mois pendant lesquels le vendeur est actif sur l'année (`13 - MONTH(BeginDate)` pour un recrutement en 2025, 12 mois pour les vendeurs présents depuis avant), puis multiplie l'objectif au prorata. Couplée à `Target_Cumul_To_Date_Adjusted` qui calcule la cible cumulée jusqu'à la date courante, cela permet à la jauge de refléter exactement *où le vendeur devrait être* à ce jour compte tenu de son ancienneté, et non pas un objectif linéaire déconnecté de la réalité.

#### Mesure clé 2 — `CA RBE Encaissé N-1` avec `TREATAS` pour préserver les filtres visuels

```dax
MEASURE 'fact_sale_order'[CA RBE Encaissé N-1] =
VAR currentMonth = MAX(fact_sale_order[custom_month_period_last_state])
VAR prevDate     = EOMONTH(currentMonth, -1)
RETURN
    CALCULATE(
        SUM(fact_sale_order[amount_untaxed]),
        dim_sale_status[state] = "sale",
        FILTER(ALL(fact_sale_order), MONTH(...) = prevMonth && YEAR(...) = prevYear),
        TREATAS(VALUES(dim_project_type[id]), fact_sale_order[project_type_id]),
        TREATAS(VALUES(dim_vendor[id]),       fact_sale_order[vendor_id])
    )
```

**Pourquoi c'est complexe :** Le `FILTER(ALL(...))` supprime les filtres de contexte pour aller chercher le mois précédent, ce qui risquerait d'ignorer les filtres de slicers actifs (type de projet, vendeur sélectionné). Le double `TREATAS` réinjecte manuellement ces filtres visuels dans le contexte de calcul modifié. C'est un pattern essentiel dès que l'on combine Time Intelligence et filtres croisés sur un modèle en étoile — sans cela, le comparatif M-1 afficherait le total de l'entreprise au lieu des données du vendeur sélectionné.

### Points Forts Visuels

- **Jauge d'objectif annuel ajustée** : Gauge Power BI affichant le CA réalisé vs l'objectif proratisé, avec un sous-titre dynamique *"Avance : X,XXX CHF"* ou *"En retard : X,XXX CHF"* selon la position du vendeur. Le texte passe au vert ou au rouge via la mise en forme conditionnelle.
- **KPIs avec variance M-1** : Chaque carte affiche la valeur du mois courant, la valeur M-1 en sous-titre, la variation absolue (Δ) et le pourcentage de variation formaté avec emoji directionnel (▲/▼).
- **Classement contextuel** : Rang du vendeur dans son agence et dans l'ensemble de l'entreprise, calculé dynamiquement selon les filtres actifs.
- **Matrice comparatif M-1** : Tableau par type de projet croisant CA M-1, CA courant et % comparatif avec mise en forme conditionnelle colorée.
- **Répartition par type de paiement et de projet** : Graphiques en anneau et en barres horizontales permettant au vendeur d'identifier son mix produit/financement.

---

## 3. Dashboard — Analyse RH

![Analyse RH — Employés, Congés et Salaires](docs/screenshots/rh_analytics.png)

### Objectif Business

Centraliser l'ensemble des indicateurs RH critiques — effectifs, contrats, absentéisme, masse salariale — dans un rapport multi-pages permettant aux RH et à la direction de suivre la santé sociale de l'entreprise sans accéder directement à Odoo.

### Pages incluses

| Page | Contenu |
|------|---------|
| **Employés** | Effectif actif, entrées/sorties, taux de rotation, répartition par poste/genre/département/âge |
| **Congés** | Absentéisme par type (maladie, accident, payé), distribution mensuelle, répartition par département |
| **Salaires** | Masse salariale totale et moyenne, évolution depuis 2023, répartition par poste/département/type de contrat |
| **Récapitulatifs** | Tableau nominatif complet avec filtres multi-axes (Année, Poste, Employé, Mois, Société, Département) |

### Analyse de l'Expertise DAX

#### Mesure clé 1 — `Average Latest Wage` : Isolation du contrat courant par `SUMMARIZE` + VAR imbriquée

```dax
MEASURE 'Fact_Employee_Activity'[Average Latest Wage] =
AVERAGEX(
    SUMMARIZE(
        'Fact_Employee_Activity',
        'Fact_Employee_Activity'[employee_id],
        "LatestWage",
        VAR LatestContractDate =
            CALCULATE(MAX('Fact_Employee_Activity'[contract_create_date]))
        RETURN
            CALCULATE(
                MAX('Fact_Employee_Activity'[wage]),
                'Fact_Employee_Activity'[contract_create_date] = LatestContractDate
            )
    ),
    [LatestWage]
)
```

**Pourquoi c'est complexe :** Un employé peut avoir plusieurs contrats successifs (CDI après CDD, augmentation…). Un simple `AVERAGE(wage)` moyennerait tous les contrats historiques, ce qui n'a aucune valeur métier. Cette mesure utilise `SUMMARIZE` pour créer une table virtuelle avec une ligne par employé, dans laquelle une variable imbriquée (`VAR LatestContractDate`) identifie d'abord la date du contrat le plus récent, puis extrait le salaire correspondant. L'`AVERAGEX` opère ensuite sur cette table agrégée, garantissant que la moyenne salariale reflète uniquement les conditions contractuelles actuelles — pas l'historique.

#### Mesure clé 2 — `Turnover Ratio (%)` : Calcul robuste du taux de rotation avec parsing de date-clé

```dax
MEASURE 'Fact_Employee_Activity'[Turnover Ratio (%)] =
VAR DepartureTable =
    ADDCOLUMNS(
        FILTER('Fact_Employee_Activity',
            'Fact_Employee_Activity'[departure_date] <> "0" ...),
        "DepartureDateParsed",
            DATE(
                VALUE(LEFT([departure_date], 4)),
                VALUE(MID([departure_date], 5, 2)),
                VALUE(RIGHT([departure_date], 2))
            )
    )
VAR DepartedEmployees = COUNTROWS(SUMMARIZE(FILTER(DepartureTable, [DepartureDateParsed] <= TODAY()), [employee_id]))
VAR ActiveEmployees   = COUNTROWS(SUMMARIZE(FILTER(..., departure_date = "0" || ISBLANK...), [employee_id]))
RETURN DIVIDE(DepartedEmployees, DIVIDE(DepartedEmployees + ActiveEmployees, 2)) * 100
```

**Pourquoi c'est complexe :** La date de départ est stockée dans le Data Warehouse au format `YYYYMMDD` (chaîne de caractères — clé de dimension date), pas comme un type `Date`. La mesure doit donc parser manuellement cette clé avec `LEFT/MID/RIGHT + VALUE + DATE` avant de pouvoir la comparer à `TODAY()`. De plus, le taux de rotation standard utilise l'effectif *moyen* (entrées + sorties / 2) comme dénominateur — et non l'effectif courant — ce qui nécessite de compter séparément les départs effectifs et les actifs, puis de diviser. La gestion explicite des sentinelles (`departure_date = "0"`, `ISBLANK`) assure la robustesse face aux données manquantes.

#### Mesure clé 3 — `Absenteeism Ratio (%)` avec `REMOVEFILTERS` pour le dénominateur

```dax
MEASURE 'Fact_Leave'[Absenteeism Ratio (%)] =
VAR TotalLeaveDays = SUM(Fact_Leave[number_of_days])
VAR Headcount =
    CALCULATE(
        DISTINCTCOUNT(Dim_Employee[employee_id]),
        REMOVEFILTERS(Fact_Leave)
    )
RETURN DIVIDE(TotalLeaveDays, Headcount * 22, 0) * 100
```

**Pourquoi c'est complexe :** Sans `REMOVEFILTERS(Fact_Leave)`, le `DISTINCTCOUNT` serait restreint aux seuls employés ayant au moins un congé dans le contexte filtré — ce qui gonflerait artificiellement le taux en excluant les employés sans absences du dénominateur. Le `REMOVEFILTERS` garantit que l'effectif total (y compris les salariés n'ayant posé aucun congé sur la période) est bien utilisé comme base de calcul. Le facteur 22 représente le nombre moyen de jours ouvrés par mois, transformant le ratio en un taux d'absentéisme mensuel normalisé, comparable aux standards RH.

### Points Forts Visuels

- **Vue Employés** : 5 KPIs en en-tête (effectif, entrées, sorties, taux de rotation, âge moyen), graphique en barres horizontales par poste, anneau par genre, anneau par société, histogramme de répartition par tranche d'âge (30-40 / 20-30 / 40-50…), barres horizontales par département. L'ensemble donne une photographie démographique instantanée de l'entreprise.
- **Vue Congés** : KPIs par type de congé (arrêt maladie, accident de travail, congés payés), double graphique en barres (nombre de demandes vs nombre de jours par type), courbe multi-séries montrant l'évolution mensuelle de chaque type d'absence depuis janvier 2025, anneau de répartition des jours par département.
- **Vue Salaires** : Histogramme de la masse salariale mensuelle depuis janvier 2023 (montrant la croissance de l'effectif sur 3 ans : 0,07M → 0,42M CHF/mois), courbe multi-années superposées (2023/2024/2025), barres horizontales par poste, camembert par type de contrat (CDI 95,23% / CDD / Stage).
- **Vue Récapitulatifs** : Tableau nominatif avec 6 slicers cascadés (Année, Poste, Employé, Mois, Société, Département) permettant de retrouver instantanément la fiche de tout employé avec date d'entrée, numéro AVS, société et salaire courant.

---

## Notes Techniques

| Aspect | Détail |
|--------|--------|
| **Connexion** | DirectQuery sur PostgreSQL DW (port 5433) |
| **Rafraîchissement** | Automatique via Power BI Service après chaque run du pipeline Airflow |
| **Horodatage** | Chaque page affiche `"Dernière mise à jour : DD/MM/YYYY HH:mm:ss"` calculé dynamiquement via `queryinsights exec_requests_history` |
| **Sécurité** | Row-Level Security (RLS) configurée sur `dim_vendor` pour que chaque commercial ne voie que ses propres données dans le Cockpit Vendeur |
| **Mesures partagées** | Les mesures `Taux de Conversion (%)`, `Total Won/Lost Opportunities` sont définies une seule fois et référencées dans les deux rapports commerciaux |

> 📁 Pour configurer Power BI avec ce Data Warehouse, voir [`docs/powerbi_setup.md`](docs/powerbi_setup.md).