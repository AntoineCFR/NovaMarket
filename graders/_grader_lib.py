# Databricks notebook source
# MAGIC %md
# MAGIC # Bibliothèque commune des graders
# MAGIC
# MAGIC Ne pas exécuter directement. Les graders l'incluent via `%run ./_grader_lib`.

# COMMAND ----------


class Grader:
    """Petit collecteur d'assertions avec rapport final."""

    OK, KO, WARN = "  OK  ", "  KO  ", " WARN "

    def __init__(self, title):
        self.title = title
        self.results = []

    # -- primitives ---------------------------------------------------------

    def _add(self, status, name, got, expected):
        self.results.append((status, name, got, expected))

    def _safe(self, fn):
        try:
            return fn(), None
        except Exception as exc:  # noqa: BLE001
            return None, f"{type(exc).__name__}: {exc}"

    # -- verifications ------------------------------------------------------

    @staticmethod
    def _with_hint(expected, hint, failed):
        """N'affiche l'indice qu'en cas d'echec : une ligne verte reste lisible."""
        return f"{expected}  <- {hint}" if (hint and failed) else expected

    def equals(self, name, fn, expected, hint=""):
        """Echec si la valeur differe de la valeur attendue."""
        got, err = self._safe(fn)
        if err:
            return self._add(self.KO, name, err, self._with_hint(expected, hint, True))
        ok = got == expected
        self._add(self.OK if ok else self.KO, name, got,
                  self._with_hint(expected, hint, not ok))

    def truthy(self, name, fn, hint=""):
        """Echec si la valeur est fausse."""
        got, err = self._safe(fn)
        if err:
            return self._add(self.KO, name, err, hint or "vrai")
        self._add(self.OK if got else self.KO, name, got, hint or "vrai")

    def soft(self, name, fn, hint=""):
        """Avertissement au lieu d'un echec : pour ce qui depend de fonctionnalites
        dont la disponibilite varie d'un workspace a l'autre."""
        got, err = self._safe(fn)
        if err:
            return self._add(self.WARN, name, f"indisponible ({err[:40]})", hint or "vrai")
        self._add(self.OK if got else self.WARN, name, got, hint or "vrai")

    def band(self, name, fn, expected, tol=0.15, hint=""):
        """Avertissement hors bande, echec seulement si la valeur est nulle ou absente.

        Utilise pour les comptages qui dependent de details d'implementation du
        runtime : on veut savoir si l'ordre de grandeur est bon sans bloquer sur
        une difference de comportement mineure.
        """
        got, err = self._safe(fn)
        attendu = f"~{expected} (±{int(tol * 100)}%)"
        if err:
            return self._add(self.KO, name, err, self._with_hint(attendu, hint, True))
        low, high = expected * (1 - tol), expected * (1 + tol)
        if not got:
            status = self.KO
        elif low <= got <= high:
            status = self.OK
        else:
            status = self.WARN
        self._add(status, name, got,
                  self._with_hint(attendu, hint, status is not self.OK))

    # -- rapport ------------------------------------------------------------

    def report(self, strict=True):
        width = max(len(name) for _s, name, _g, _e in self.results) + 2
        print("=" * (width + 46))
        print(f" {self.title}")
        print("=" * (width + 46))
        for status, name, got, expected in self.results:
            print(f"[{status}] {name:<{width}} obtenu={str(got)[:26]:<28} attendu={expected}")
        print("-" * (width + 46))

        n_ko = sum(1 for s, *_ in self.results if s == self.KO)
        n_warn = sum(1 for s, *_ in self.results if s == self.WARN)
        n_ok = len(self.results) - n_ko - n_warn
        print(f" {n_ok} réussi(s) · {n_warn} avertissement(s) · {n_ko} échec(s)")
        print("=" * (width + 46))

        if n_ko and strict:
            raise AssertionError(f"{n_ko} critère(s) non satisfait(s) — le module n'est pas validé.")
        if n_ko == 0:
            print("\n  Module validé.\n")
        return n_ko == 0
