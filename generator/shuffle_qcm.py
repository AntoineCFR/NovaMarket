# -*- coding: utf-8 -*-
"""Redistribue les options des examens blancs : la bonne reponse etait en B dans
41 cas sur 45, ce qui permettait de passer l'examen sans le lire."""
import io, re, sys, random

LETTERS = "ABCD"
OPT = re.compile(r"^- \*\*([A-D])\.\*\* (.*)$")
QNUM = re.compile(r"^\*\*(\d+)\.\*\* ")
KEYROW = re.compile(r"^\| (\d+) \| ([A-D]) \| (.*)$")

def balanced(n, seed):
    """Suite de n lettres, equilibree, sans plus de 2 identiques d'affilee."""
    rnd = random.Random(seed)
    base = [LETTERS[i % 4] for i in range(n)]
    for _ in range(500):
        rnd.shuffle(base)
        if not any(base[i] == base[i+1] == base[i+2] for i in range(n-2)):
            return base
    return base

def process(path, seed):
    lines = io.open(path, encoding="utf-8").read().split("\n")
    key = {}
    for ln in lines:
        m = KEYROW.match(ln)
        if m:
            key[int(m.group(1))] = m.group(2)
    n = len(key)
    targets = dict(zip(sorted(key), balanced(n, seed)))
    rnd = random.Random(seed + 1)

    out, i, seen = [], 0, set()
    while i < len(lines):
        m = QNUM.match(lines[i])
        # bloc d'options : 4 lignes consecutives '- **X.**'
        if m and int(m.group(1)) in key and int(m.group(1)) not in seen:
            q = int(m.group(1)); seen.add(q)
            out.append(lines[i]); i += 1
            while i < len(lines) and not OPT.match(lines[i]):
                out.append(lines[i]); i += 1
            opts = {}
            start = i
            while i < len(lines) and OPT.match(lines[i]):
                mm = OPT.match(lines[i]); opts[mm.group(1)] = mm.group(2); i += 1
            if len(opts) != 4:
                sys.exit("Q%d : %d options a la ligne %d" % (q, len(opts), start))
            good = opts.pop(key[q])
            distract = list(opts.values()); rnd.shuffle(distract)
            new = {targets[q]: good}
            for L in LETTERS:
                if L != targets[q]:
                    new[L] = distract.pop()
            for L in LETTERS:
                out.append("- **%s.** %s" % (L, new[L]))
            continue
        m = KEYROW.match(lines[i])
        if m:
            q = int(m.group(1))
            out.append("| %d | %s | %s" % (q, targets[q], m.group(3)))
            i += 1
            continue
        out.append(lines[i]); i += 1

    if len(seen) != n:
        sys.exit("%s : %d blocs d'options pour %d reponses" % (path, len(seen), n))
    io.open(path, "w", encoding="utf-8", newline="").write("\n".join(out))
    from collections import Counter
    print("%s : %d questions, repartition %s"
          % (path, n, dict(sorted(Counter(targets.values()).items()))))

process("mock-exam-1.md", 20260903)
process("mock-exam-2.md", 20260904)
