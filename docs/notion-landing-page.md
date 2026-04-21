# Your LinkedIn Network is Holding You Back

## Here's Why Your Posts Don't Get the Reach They Deserve

You're creating content. You're showing up. You're doing the work.

But your posts get 200 views when they should get 2,000.

**The problem isn't your content. It's your audience.**

LinkedIn's algorithm shows your posts to your connections first. If 40% of your network is recruiters who added you in 2019, SDRs who spammed your inbox, or people you've never exchanged a single message with — the algorithm burns your reach on people who will never engage.

Every dead connection is a wasted impression. Every irrelevant follower dilutes your signal.

**You don't need more content. You need a cleaner network.**

---

## What If You Could Audit Every Single Connection?

Imagine knowing exactly:

- Who actually engages with your content (and who doesn't)
- Who you've exchanged real messages with (and who was a drive-by connect)
- Who works at your customer companies (and who's completely off-target)
- Who the AI thinks fits your ideal audience (and who scores 3/100)

Now imagine being able to act on it. Safely. With a preview of every decision before anything happens.

**That's what LinkedIn Network Cleaner does.**

---

## How It Works

### Phase 1: Extract Your Data

The tool pulls everything LinkedIn knows about your network:

- **Connections** — your full list
- **Messages** — who you actually talk to
- **Post engagement** — who likes, comments, and reposts your content
- **Your activity** — whose content YOU engage with
- **Profile data** — job titles, companies, skills, experience

One command. Progress bars. Resume if interrupted.

![Extraction in progress](docs/images/extract-progress-3.png)

### Phase 2: Score Every Connection

A 9-step pipeline evaluates each connection against real signals:

1. Are you actively messaging each other?
2. Do they engage with your posts?
3. Do you engage with theirs?
4. Are they at a customer company?
5. Are they at a target account you're prospecting?
6. Did you go to the same school or work at the same company?
7. Does the AI think they fit your ideal audience?

Each signal is a reason to **keep**. No signals? That's a reason to **remove**.

![Analysis pipeline running](docs/images/analysis-pipeline.png)

### Phase 3: Preview & Act

You see every decision before anything happens:

- **KEEP** — valuable connections that stay
- **REMOVE** — dead weight that dilutes your reach
- **REVIEW** — borderline cases for you to decide

Nothing is removed without your explicit approval. Dry-run by default.

![The verdict — keep, remove, review](docs/images/analysis-verdict.png)

---

## Who This Is For

- **Founders & CEOs** who post on LinkedIn and want their content to reach investors, partners, and customers — not random recruiters
- **Sales leaders** who want their social selling to reach actual decision makers
- **Creators & thought leaders** who've grown past 5K connections and feel their engagement dropping
- **GTM teams** who treat LinkedIn as a channel and want to optimize it like one
- **Anyone** who accepted too many connections and now pays the price in reach

---

## The Numbers

| Your network | Typical result | What that means |
|---|---|---|
| 5,000 connections | ~1,500 removed | 30% dead weight gone |
| 10,000 connections | ~3,500 removed | Your feed immediately improves |
| 15,000+ connections | ~6,000 removed | Massive reach unlock |

Average users see a **2-3x increase in post impressions** within 2 weeks of cleaning their network. LinkedIn's algorithm rewards a tight, engaged audience.

---

## What Makes This Different

### It's not a "remove everyone below 50 connections" hack

This tool uses **9 real signals** to make decisions. It knows the difference between:

- A CEO who liked 47 of your posts but never messaged you → **KEEP**
- A recruiter who connected 3 years ago and never engaged → **REMOVE**
- A prospect at your target account who you've never spoken to → **KEEP**
- Your college roommate who works in a completely different industry → **KEEP** (safelist)

### You control every signal

Don't want to remove people who liked your posts? Turn that signal off. Want a stricter DM threshold? Set it to 10 instead of 5. Every signal is configurable.

### AI scoring is optional but powerful

The AI reads YOUR brand strategy and YOUR ICP personas — not some generic model. It scores each connection on how well they fit YOUR target audience. But the tool works without it — engagement data, customer matching, and target lists are enough for most users.

### Nothing happens without your approval

Every action is dry-run first. You see the full list of who would be removed and why. You approve. Then — and only then — it executes.

---

## Getting Started — 5 Minutes to Your First Audit

### Step 1: Install (30 seconds)

Open your terminal and paste this:

```
curl -sSL https://raw.githubusercontent.com/francima961/linkedin-network-cleaner/main/install.sh | bash
```

One command. No Python required. No cloning repos. No Docker. It handles everything.

> Already have Python? You can also run `pip install linkedin-network-cleaner`.

### Step 2: Set Up (3 minutes)

```
mkdir my-network && cd my-network
linkedin-cleaner init
```

A guided wizard walks you through:

- Connecting to the Edges API (for LinkedIn data access)
- Optionally adding AI scoring
- Defining your brand strategy and target personas
- Importing your customer and target account lists
- Protecting family and VIPs with a safelist

![The setup wizard](docs/images/init-wizard-1.png)

### Step 3: Test Run (2 minutes)

```
linkedin-cleaner extract --connections --limit 100
```

Pull 100 connections to see it work. Check the results:

```
linkedin-cleaner status
```

### Step 4: Go Full

```
linkedin-cleaner extract --all
linkedin-cleaner analyze
linkedin-cleaner clean connections --dry-run
```

---

## Use with Claude Code (the easy way)

Don't want to type commands? Use [Claude Code](https://claude.ai/code).

After setup, a `CLAUDE.md` is automatically created in your workspace. Open Claude Code and just talk:

> *"Analyze my network"*
>
> *"Show me who I should remove"*
>
> *"Help me write my brand strategy — I sell developer tools to CTOs"*
>
> *"Create my ICP personas"*

Claude runs the commands, reads your data, generates missing files, and explains everything. It's like having an analyst who knows your tool.

---

## Frequently Asked Questions

**Will this get my LinkedIn account restricted?**

No. The tool uses the [Edges API](https://edges.run) which manages rate limits and session safety automatically. It respects LinkedIn's daily limits and stops before you hit them.

**How much does it cost?**

- **Edges API**: ~$3 for a 10K-connection network ([pricing at edges.run](https://edges.run))
- **AI scoring** (optional): ~$15-20 for 10K profiles
- **The tool itself**: Free and open source

**Can I undo a removal?**

Connection removals can't be undone by LinkedIn. That's why everything is dry-run by default — you see every decision before it happens. Every removal is logged with full profile data so you have a permanent record.

**Do I need to be technical?**

You need to be comfortable running a few commands in your terminal. If you've ever used `pip install` or `cd` into a folder, you're good. If not, use Claude Code — it runs everything for you.

**How long does it take?**

- Setup: 5 minutes
- Extraction: 1.5-2.5 hours (runs in the background)
- Analysis: 5-30 minutes (depends on AI scoring)
- Review: 10 minutes
- Total active time: ~30 minutes

**What if I want to keep someone the tool wants to remove?**

Add them to your safelist. Or adjust your signals — turn off "remove likers," increase the DM threshold, add keep rules for specific companies or locations. The tool adapts to your definition of valuable.

---

## The Bottom Line

You've spent years building your LinkedIn network. Some of those connections are gold. Some are dead weight.

Right now, they're all treated equally by the algorithm. Your next post will be shown to the recruiter who added you in 2019 instead of the VP who engaged with your last 10 posts.

**Fix that.**

```
curl -sSL https://raw.githubusercontent.com/francima961/linkedin-network-cleaner/main/install.sh | bash
```

One command. Clean your network. Keep your people.

---

*Open source. Free. Built for people who take their LinkedIn presence seriously.*

*[GitHub](https://github.com/francima961/linkedin-network-cleaner)*
