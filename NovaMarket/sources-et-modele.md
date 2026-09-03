# Sources de données et objectif métier

> ⚠️ Ce document est le **contrat d'interface déclaré par les équipes source**.
> C'est ce qu'elles affirment produire. Une partie non négligeable du travail
> d'un Data Engineer consiste à découvrir l'écart entre ce contrat et la réalité.
> Ne prends rien pour acquis.

---

## S1 — Commandes (export backoffice)

Fichiers CSV déposés dans le Volume, un par mois pour l'historique puis un par jour.

| Propriété | Valeur |
|---|---|
| Chemin | `/Volumes/novamarket/landing/files/orders/` |
| Nommage | `orders_YYYY-MM.csv` (historique) · `orders_YYYY-MM-DD.csv` (quotidien) |
| Séparateur | `;` |
| Encodage | `windows-1252` (export d'un ERP Windows) |
| En-tête | oui, première ligne |
| Fin de ligne | CRLF |
| Granularité | **une ligne = une ligne de commande** (l'en-tête de commande est répété) |

| Colonne | Type déclaré | Description |
|---|---|---|
| `order_id` | string | Identifiant de la commande |
| `order_line_id` | string | Identifiant de la ligne, `{order_id}-{n}`. Clé unique déclarée |
| `order_ts` | timestamp | Horodatage de la commande, `yyyy-MM-dd HH:mm:ss` |
| `customer_id` | string | FK vers le client |
| `seller_id` | string | FK vers le vendeur |
| `product_id` | string | FK vers le produit |
| `quantity` | int | Quantité commandée, > 0 |
| `unit_price` | decimal | Prix unitaire TTC, **virgule décimale** |
| `discount_amount` | decimal | Remise appliquée sur la ligne, virgule décimale |
| `currency` | string | Toujours `EUR` |
| `shipping_country` | string | Code ISO-2 |
| `payment_method` | string | `CARD`, `PAYPAL`, `TRANSFER`, `GIFTCARD`, `APPLEPAY` |
| `order_status` | string | `DELIVERED`, `SHIPPED`, `PENDING`, `CANCELLED`, `RETURNED` |
| `shipping_address` | string | Adresse de livraison en texte libre |

**Chiffre d'affaires d'une ligne** = `quantity * unit_price - discount_amount`.
Les statuts `CANCELLED` et `RETURNED` ne contribuent pas au CA net.

---

## S2 — Événements applicatifs (clickstream)

Export quotidien du collecteur d'événements web et mobile.

| Propriété | Valeur |
|---|---|
| Chemin | `/Volumes/novamarket/landing/files/events/` |
| Nommage | `events_YYYY-MM-DD.jsonl.gz` |
| Format | JSON Lines, **gzippé**, UTF-8, un objet par ligne |

Structure imbriquée :

```json
{
  "event_id": "E-20260602-0000000",
  "event_ts": "2026-06-02T14:19:32Z",
  "event_type": "page_view | search | product_view | add_to_cart | checkout_start | purchase",
  "user":    { "customer_id": "C002421", "session_id": "S-...", "segment": "STANDARD" },
  "device":  { "os": "iOS", "app_version": "4.1.3", "is_mobile": true },
  "context": { "page": "/p/P003932", "referrer": "...",
               "utm": { "source": "...", "medium": "...", "campaign": "..." } },
  "search_term": null,
  "items":   [ { "product_id": "P005948", "qty": 3, "price": 6.97 } ],
  "order_id": null
}
```

`items` n'est rempli que pour `add_to_cart`, `checkout_start` et `purchase`.
`order_id` n'est rempli que pour `purchase`.
`user.customer_id` est nul pour les visiteurs non authentifiés.

Les événements d'une même **session** (`user.session_id`) partagent le visiteur, le
terminal et la source d'acquisition. Une session parcourt tout ou partie de l'entonnoir
`page_view → search → product_view → add_to_cart → checkout_start → purchase`.

---

## S3 — Référentiels (catalogue)

| Fichier | Chemin | Format |
|---|---|---|
| `categories.csv` | `/Volumes/novamarket/landing/files/ref/` | CSV, séparateur `,`, **UTF-8**, quoting standard |
| `sellers.csv` | idem | idem |
| `products.csv` | idem | idem |

`categories` : `category_id`, `category_label`, `top_category_code`, `top_category_label`
`sellers` : `seller_id`, `seller_name`, `seller_country`, `seller_city`, `main_top_category`, `plan_code`, `is_active`, `onboarded_at`
`products` : `product_id`, `product_name`, `brand`, `category_id`, `seller_id`, `list_price`, `weight_kg`, `is_discontinued`

Ces fichiers sont **rechargés intégralement** à chaque livraison (snapshot complet, pas de delta).

---

## S4 — Base OLTP applicative (à partir de M2)

Base Postgres managée (Lakebase), tables `app_customers` et `app_sellers`, avec une
colonne `updated_at` et un drapeau `is_deleted`. C'est la source de vérité pour les
clients et le plan d'abonnement des vendeurs. Elle évolue dans le temps → historisation
requise.

---

## Grille de commission

Le taux de commission dépend du plan du vendeur **au moment de la commande** :

| Plan | Taux |
|---|---|
| `BASIC` | 15,0 % |
| `PLUS` | 11,5 % |
| `PREMIUM` | 8,5 % |

C'est le point qui rend l'historisation SCD2 (M4) non négociable : un vendeur qui
passe de `BASIC` à `PREMIUM` en avril ne doit pas voir sa commission de janvier recalculée.

---

## Objectif final : les 6 questions du layer gold

Ton socle `gold` doit permettre de répondre à ces questions **sans jointure exotique
ni retraitement** de la part des analystes :

1. **CA net et commission** par mois, par catégorie de tête et par vendeur.
2. **Taux d'annulation et taux de retour** par catégorie et par vendeur, par mois.
3. **Panier moyen et nombre de commandes** par segment client et par pays.
4. **Cohortes d'acquisition** : rétention des clients par mois de première commande.
5. **Entonnoir de conversion** issu du clickstream : `product_view → add_to_cart → checkout_start → purchase`, par canal d'acquisition (utm_source).
6. **Top 20 des produits** par CA net sur les 90 derniers jours, avec leur taux de retour.

Chaque question devra être servie par une table ou une vue `gold` documentée
(commentaires UC sur la table et sur chaque colonne).
