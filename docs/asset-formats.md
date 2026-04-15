# Asset File Formats

This document describes the format and purpose of each user-provided asset file. These files live in your workspace's `assets/` directory and are used by the analysis pipeline to score and classify your LinkedIn connections.

---

## Brand Strategy

**Location:** `assets/brand_strategy.md` (or any `.md` file with "brand" in the filename)

**Purpose:** Fed to the AI scorer as context for evaluating how well each LinkedIn connection fits your target audience. The more specific your brand strategy, the more accurate the AI scoring.

**Format:** Markdown, any structure. The AI reads the entire file as context — there is no required schema. At minimum, include your company description, target market, and value proposition.

**Auto-detection:** Any `.md` file in `assets/` with "brand" in the filename (case-insensitive).

**Example:**
```markdown
## Company Overview
Acme Analytics provides real-time dashboards for mid-market retailers.

## Target Market
Retail and e-commerce companies, 100–2,000 employees, North America.

## Value Proposition
We unify fragmented retail data into a single real-time view,
deploying in days without requiring a dedicated data team.
```

---

## ICP / Persona Definition

**Location:** `assets/persona.md`, `Persona*.md`, or any `.md` file with "persona" or "icp" in the filename

**Purpose:** Defines your target personas so the AI scorer can classify each LinkedIn connection into a persona category and assign an appropriate fit score.

**Format:** Markdown with persona definitions. Each persona should include role patterns (job titles), company characteristics, and a fit score range.

**Auto-detection:** Any `.md` file in `assets/` with "persona" or "icp" in the filename (case-insensitive).

**Example:**
```markdown
## Persona 1: Decision Maker
Role patterns: VP of Sales, Director of Marketing, Head of Revenue
Company: SaaS companies, 100–1,000 employees
Fit score: 80–100
```

---

## Customer Companies

**Location:** `assets/Customers/*.csv`

**Purpose:** Match connections who work at your existing customer companies. Used in analysis pipeline step 6 to flag connections at known customers (these are typically kept regardless of other scores).

**Required column:** `company_name` (auto-detected, case-insensitive). Also accepts: `Company Name`, `Company`, `company`.

**Format:** CSV with one company per row.

**Example:**
```csv
company_name
Acme Corporation
TechStart Inc
Global Solutions Ltd
```

---

## Target Accounts

**Location:** `assets/Accounts/*.csv`

**Purpose:** Match connections at companies you are actively targeting. Used in analysis pipeline step 7 to boost the score of connections at target accounts.

**Required column:** Company name (auto-detected from: `Company`, `Company Name`, `company_name`, `Organization`, `Account Name`).

**Format:** CSV with one company per row. Additional columns (Industry, Employee Count, etc.) are optional and ignored by the matcher.

**Example:**
```csv
Company,Industry,Employee Count
Acme Corporation,Technology,500
TechStart Inc,SaaS,120
Global Solutions Ltd,Consulting,2000
```

---

## Target Prospects

**Location:** `assets/Prospects/*.csv`

**Purpose:** Match specific people by their LinkedIn profile ID. Used in analysis pipeline step 8 to flag high-priority individuals regardless of their company or title.

**Required column:** LinkedIn profile ID (auto-detected from: `Person Linkedin Id`, `LinkedIn Member ID`, `linkedin_profile_id`, `LinkedIn ID`, `li_id`).

**Optional column:** ICP tag (auto-detected from: `ICP Tag`, `persona_tag`, `Persona`, `icp_tag`). If present, the tag is applied to the matched connection.

**Format:** CSV with one person per row.

**Example:**
```csv
Person Linkedin Id,First Name,Last Name,Company,Title
123456789,Jane,Smith,Acme Corporation,VP of Engineering
234567890,John,Doe,TechStart Inc,Head of Growth
345678901,Sarah,Johnson,Global Solutions Ltd,Director of Operations
```
