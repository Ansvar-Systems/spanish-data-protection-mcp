#!/usr/bin/env tsx
/**
 * AEPD (Agencia Española de Protección de Datos) ingestion crawler.
 *
 * Scrapes the AEPD website (aepd.es) for:
 *   - Resoluciones / procedimientos sancionadores (PS- decisions)
 *   - Procedimientos de derechos (PD- decisions)
 *   - Apercibimientos e instrucciones (AI- decisions)
 *   - Guías y herramientas (guides, circulares, informes)
 *
 * Populates the SQLite database used by the MCP server.
 *
 * Usage:
 *   npx tsx scripts/ingest-aepd.ts                  # Full ingestion
 *   npx tsx scripts/ingest-aepd.ts --resume          # Skip already-ingested references
 *   npx tsx scripts/ingest-aepd.ts --dry-run          # Parse and log, do not write to DB
 *   npx tsx scripts/ingest-aepd.ts --force            # Drop existing data and re-ingest
 *
 * Environment:
 *   AEPD_DB_PATH      — SQLite database path (default: data/aepd.db)
 *   AEPD_USER_AGENT   — Custom User-Agent header (default: built-in)
 *   AEPD_RATE_LIMIT   — Milliseconds between requests (default: 1500)
 *   AEPD_MAX_RETRIES  — Max retry attempts per request (default: 3)
 *   AEPD_MAX_PAGES    — Max listing pages to crawl per source (default: 50)
 */

import Database from "better-sqlite3";
import { existsSync, mkdirSync, unlinkSync } from "node:fs";
import { dirname } from "node:path";
import { SCHEMA_SQL } from "../src/db.js";

// ---------------------------------------------------------------------------
// cheerio — loaded dynamically so the script fails fast with a clear message
// ---------------------------------------------------------------------------

// eslint-disable-next-line @typescript-eslint/no-explicit-any
let cheerio: any;
try {
  // Dynamic import — cheerio is an optional dev dependency for ingestion only
  cheerio = await (Function('return import("cheerio")')() as Promise<any>);
} catch {
  console.error(
    "Missing dependency: cheerio\n" +
      "Install it with:  npm install --save-dev cheerio @types/cheerio\n",
  );
  process.exit(1);
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

const DB_PATH = process.env["AEPD_DB_PATH"] ?? "data/aepd.db";
const USER_AGENT =
  process.env["AEPD_USER_AGENT"] ??
  "AnsvarAEPDCrawler/1.0 (+https://ansvar.eu; data-protection-research)";
const RATE_LIMIT_MS = parseInt(
  process.env["AEPD_RATE_LIMIT"] ?? "1500",
  10,
);
const MAX_RETRIES = parseInt(
  process.env["AEPD_MAX_RETRIES"] ?? "3",
  10,
);
const MAX_PAGES = parseInt(
  process.env["AEPD_MAX_PAGES"] ?? "50",
  10,
);

const BASE_URL = "https://www.aepd.es";

// CLI flags
const args = new Set(process.argv.slice(2));
const FLAG_RESUME = args.has("--resume");
const FLAG_DRY_RUN = args.has("--dry-run");
const FLAG_FORCE = args.has("--force");

// ---------------------------------------------------------------------------
// Known AEPD decision and guidance URLs
//
// The AEPD publishes resolutions as PDF documents at /documento/<ref>.pdf.
// Listing pages at /informes-y-resoluciones/resoluciones use Drupal Views
// with .views-row items. We crawl listing pages to discover references,
// then fetch each PDF's metadata from the listing entry.
//
// For guides, /guias-y-herramientas/guias lists guides with pagination.
// Guide PDFs are at /guias/<slug>.pdf or /documento/<slug>.pdf.
//
// When listing page crawling fails or returns partial results, the crawler
// falls back to this curated index of known high-value decisions.
// ---------------------------------------------------------------------------

/** Decision source entry — URL path + optional pre-known metadata. */
interface DecisionSource {
  /** PDF path relative to BASE_URL, e.g. "/documento/ps-00120-2022.pdf" */
  pdfPath: string;
  /** Stable reference ID, e.g. "PS-00120-2022". Derived from pdfPath if absent. */
  reference?: string | undefined;
  /** Decision type hint — overridden by reference prefix if available. */
  type?: "sancion" | "apercibimiento" | "derechos" | "reposicion" | "tutela" | "informe" | undefined;
  /** Date in YYYY-MM-DD format if known from listing page. */
  date?: string | undefined;
  /** Title text from listing page. */
  title?: string | undefined;
}

/** Guideline source entry. */
interface GuidelineSource {
  /** URL path (HTML page or PDF) */
  url: string;
  reference?: string | undefined;
  type?: "guia" | "informe" | "circular" | "recomendacion" | "herramienta" | "orientacion" | undefined;
  title?: string | undefined;
  date?: string | undefined;
}

// -- Curated decision URLs (major sanctions and notable decisions) -----------

const KNOWN_DECISIONS: DecisionSource[] = [
  // CaixaBank — EUR 6M (commercial communications, profiling)
  { pdfPath: "/documento/ps-00120-2022.pdf", reference: "PS-00120-2022", type: "sancion", date: "2023-01-27" },
  // Vodafone España — EUR 8.15M (commercial communications, data processing)
  { pdfPath: "/documento/ps-00210-2022.pdf", reference: "PS-00210-2022", type: "sancion", date: "2022-12-21" },
  // La Liga — EUR 250K (microphone app)
  { pdfPath: "/documento/ps-00131-2019.pdf", reference: "PS-00131-2019", type: "sancion", date: "2019-06-11" },
  // BBVA — EUR 5M (international transfers, commercial comms)
  { pdfPath: "/documento/ps-00081-2021.pdf", reference: "PS-00081-2021", type: "sancion", date: "2021-04-29" },
  // Equifax Ibérica — EUR 1M (credit files, data subjects' rights)
  { pdfPath: "/documento/ps-00353-2019.pdf", reference: "PS-00353-2019", type: "sancion", date: "2019-11-28" },
  // Google — EUR 10M (right to be forgotten)
  { pdfPath: "/documento/ps-00112-2021.pdf", reference: "PS-00112-2021", type: "sancion" },
  // Mercadona — EUR 2.52M (facial recognition CCTV)
  { pdfPath: "/documento/ps-00120-2021.pdf", reference: "PS-00120-2021", type: "sancion" },
  // Glovo — EUR 550K (data breach, riders' data)
  { pdfPath: "/documento/ps-00150-2022.pdf", reference: "PS-00150-2022", type: "sancion" },
  // EDP Energía — EUR 1.5M (fraudulent contracts)
  { pdfPath: "/documento/ps-00218-2021.pdf", reference: "PS-00218-2021", type: "sancion" },
  // Endesa — EUR 500K
  { pdfPath: "/documento/ps-00396-2021.pdf", reference: "PS-00396-2021", type: "sancion" },
  // Iberdrola — EUR 3.15M (customer data)
  { pdfPath: "/documento/ps-00066-2022.pdf", reference: "PS-00066-2022", type: "sancion" },
  // WiZink — EUR 4M (commercial communications)
  { pdfPath: "/documento/ps-00117-2022.pdf", reference: "PS-00117-2022", type: "sancion" },
  // Caixabank — EUR 2.1M (deber de informar)
  { pdfPath: "/documento/ps-00386-2022.pdf", reference: "PS-00386-2022", type: "sancion" },
  // Telefónica Móviles — EUR 900K (fraudulent portabilities)
  { pdfPath: "/documento/ps-00037-2020.pdf", reference: "PS-00037-2020", type: "sancion" },
  // BBVA — EUR 3M
  { pdfPath: "/documento/ps-00413-2021.pdf", reference: "PS-00413-2021", type: "sancion" },
  // Orange — EUR 700K
  { pdfPath: "/documento/ps-00253-2023.pdf", reference: "PS-00253-2023", type: "sancion" },
  // Recent 2025-2026 decisions
  { pdfPath: "/documento/ps-00211-2025.pdf", reference: "PS-00211-2025", type: "sancion" },
  { pdfPath: "/documento/ps-00200-2024.pdf", reference: "PS-00200-2024", type: "sancion" },
  { pdfPath: "/documento/ps-00524-2025.pdf", reference: "PS-00524-2025", type: "sancion" },
];

// -- Curated guideline URLs -------------------------------------------------

const KNOWN_GUIDELINES: GuidelineSource[] = [
  // AI / Agentic AI
  { url: "/guias/orientaciones-ia-agentica.pdf", type: "guia", title: "Inteligencia artificial agéntica desde la perspectiva de la protección de datos", date: "2026-02-18" },
  // AI recommendations
  { url: "/guias/recomendaciones-ia-aepd.pdf", type: "guia", title: "Cuidado con lo que le confIAs", date: "2026-01-27" },
  // IAG summary
  { url: "/guias/sumario-recomendaciones-iag-aepd.pdf", type: "guia", title: "Resumen básico de obligaciones y recomendaciones para la gestión de IAG en la AEPD", date: "2026-01-19" },
  // Images in AI
  { url: "/guias/guia-aepd-uso-de-imagenes-de-terceros-en-sistemas-ia.pdf", type: "guia", title: "El uso de imágenes de terceros en sistemas de inteligencia artificial", date: "2026-01-13" },
  // Data protection in labour relations
  { url: "/guias/la-proteccion-de-datos-en-las-relaciones-laborales.pdf", type: "guia", title: "La protección de datos en las relaciones laborales", date: "2025-12-17" },
  // Encryption for SMEs
  { url: "/guias/guia-cifrado-autonomos-pymes.pdf", type: "guia", title: "Guía de cifrado para autónomos y pymes", date: "2025-11-18" },
  // Cookies guide
  { url: "/guias/guia-cookies.pdf", type: "guia", title: "Guía sobre el uso de las cookies" },
  // Videovigilancia guide
  { url: "/guias/guia-videovigilancia.pdf", type: "guia", title: "Guía sobre videovigilancia" },
  // DPIA guide
  { url: "/guias/guia-evaluaciones-de-impacto.pdf", type: "guia", title: "Guía práctica para las Evaluaciones de Impacto sobre la Protección de Datos" },
  // Deber de informar guide
  { url: "/guias/guia-modelo-clausula-informativa.pdf", type: "guia", title: "Guía para el cumplimiento del deber de informar" },
  // Data protection by default
  { url: "/documento/guia-proteccion-datos-por-defecto.pdf", type: "guia", title: "Guía de Protección de Datos por Defecto", date: "2020-10-01" },
  // Local administration guide
  { url: "/guias/guia-proteccion-datos-administracion-local.pdf", type: "guia", title: "Protección de Datos y Administración Local" },
  // DPO guide
  { url: "/guias/guia-figura-del-delegado-de-proteccion-de-datos.pdf", type: "guia", title: "Guía sobre la figura del Delegado de Protección de Datos" },
  // Risk analysis
  { url: "/guias/guia-analisis-de-riesgos.pdf", type: "guia", title: "Guía de análisis de riesgos en protección de datos" },
  // Security breach management
  { url: "/guias/guia-brechas-seguridad.pdf", type: "guia", title: "Guía para la gestión y notificación de brechas de seguridad" },
  // Children and adolescents
  { url: "/guias/guia-proteccion-datos-menores-adolescentes.pdf", type: "guia", title: "Protección de datos de menores y adolescentes" },
  // Healthcare data
  { url: "/guias/guia-proteccion-datos-investigacion-salud.pdf", type: "guia", title: "Protección de datos en la investigación en salud" },
  // Public administration guides
  { url: "/guias/guia-privacidad-desde-diseno.pdf", type: "guia", title: "Guía de privacidad desde el diseño" },
];

// ---------------------------------------------------------------------------
// Listing page URLs — used for dynamic discovery
// ---------------------------------------------------------------------------

/** Listing pages for resoluciones (decisions). AEPD uses ?page=N (0-indexed). */
const DECISION_LIST_BASE = "/informes-y-resoluciones/resoluciones";

/** Listing pages for guías (guidelines). */
const GUIDELINE_LIST_BASE = "/guias-y-herramientas/guias";

// ---------------------------------------------------------------------------
// Topic detection — maps Spanish keywords to topic IDs
// ---------------------------------------------------------------------------

interface TopicRule {
  id: string;
  name_es: string;
  name_en: string;
  description: string;
  /** Keywords to match in title + summary + full_text (case-insensitive). */
  keywords: string[];
}

const TOPIC_RULES: TopicRule[] = [
  {
    id: "videovigilancia",
    name_es: "Videovigilancia y CCTV",
    name_en: "Video surveillance and CCTV",
    description:
      "Uso de sistemas de videovigilancia en espacios públicos, lugares de trabajo y establecimientos comerciales (art. 22 LOPDGDD).",
    keywords: [
      "videovigilancia", "cctv", "cámara", "video", "vigilancia",
      "grabación", "imágenes", "reconocimiento facial",
    ],
  },
  {
    id: "cookies",
    name_es: "Cookies y rastreadores",
    name_en: "Cookies and trackers",
    description:
      "Instalación y uso de cookies y tecnologías de rastreo en dispositivos de usuarios (art. 22 LSSI y RGPD).",
    keywords: [
      "cookie", "cookies", "rastreador", "tracker", "tracking",
      "lssi", "sociedad de la información",
    ],
  },
  {
    id: "telecomunicaciones",
    name_es: "Telecomunicaciones y comunicaciones electrónicas",
    name_en: "Telecommunications and electronic communications",
    description:
      "Protección de datos en el sector de las telecomunicaciones, incluyendo operadores móviles y proveedores de servicios de internet.",
    keywords: [
      "telecomunicación", "telecomunicaciones", "operador",
      "vodafone", "telefónica", "orange", "movistar", "jazztel",
      "portabilidad", "línea móvil", "fibra",
    ],
  },
  {
    id: "publicidad",
    name_es: "Publicidad y marketing directo",
    name_en: "Advertising and direct marketing",
    description:
      "Comunicaciones comerciales y publicidad personalizada, incluyendo el envío de comunicaciones electrónicas sin consentimiento.",
    keywords: [
      "publicidad", "marketing", "comunicación comercial",
      "comunicaciones comerciales", "spam", "correo electrónico",
      "sms", "lista robinson", "llamadas comerciales",
    ],
  },
  {
    id: "derechos_interesados",
    name_es: "Derechos de los interesados",
    name_en: "Data subject rights",
    description:
      "Ejercicio de los derechos de acceso, rectificación, supresión, limitación, portabilidad y oposición (art. 15-22 RGPD).",
    keywords: [
      "derecho de acceso", "rectificación", "supresión",
      "derecho al olvido", "limitación", "portabilidad",
      "oposición", "derechos del interesado", "derechos arco",
      "derecho de cancelación",
    ],
  },
  {
    id: "evaluacion_impacto",
    name_es: "Evaluación de Impacto sobre la Protección de Datos (EIPD)",
    name_en: "Data Protection Impact Assessment (DPIA)",
    description:
      "Evaluación de riesgos para los derechos y libertades de los interesados en tratamientos de alto riesgo (art. 35 RGPD).",
    keywords: [
      "evaluación de impacto", "eipd", "dpia",
      "análisis de riesgos", "alto riesgo",
    ],
  },
  {
    id: "transferencias",
    name_es: "Transferencias internacionales",
    name_en: "International transfers",
    description:
      "Transferencias de datos personales a terceros países u organizaciones internacionales (art. 44-49 RGPD).",
    keywords: [
      "transferencia internacional", "tercer país", "terceros países",
      "cláusulas contractuales tipo", "decisión de adecuación",
      "escudo de privacidad", "privacy shield", "schrems",
      "normas corporativas vinculantes", "bcr",
    ],
  },
  {
    id: "consentimiento",
    name_es: "Consentimiento",
    name_en: "Consent",
    description:
      "Validez del consentimiento como base legal para el tratamiento de datos, incluyendo el consentimiento libre, específico, informado e inequívoco (art. 6 y 7 RGPD).",
    keywords: [
      "consentimiento", "consent", "consentir",
      "consentimiento informado", "opt-in", "opt-out",
      "consentimiento expreso",
    ],
  },
  {
    id: "menores",
    name_es: "Menores de edad",
    name_en: "Minors",
    description:
      "Protección de datos de menores de edad en servicios de la sociedad de la información (art. 8 RGPD y art. 7 LOPDGDD — mayoría de edad digital a los 14 años en España).",
    keywords: [
      "menor", "menores", "niño", "adolescente",
      "edad digital", "centro escolar", "colegio",
    ],
  },
  {
    id: "brechas_seguridad",
    name_es: "Brechas de seguridad",
    name_en: "Data breaches",
    description:
      "Notificación y gestión de brechas de seguridad que afectan a datos personales (art. 33-34 RGPD).",
    keywords: [
      "brecha de seguridad", "brecha", "filtración",
      "violación de seguridad", "incidente de seguridad",
      "notificación de brecha", "data breach", "quiebra de seguridad",
    ],
  },
  {
    id: "laboral",
    name_es: "Protección de datos en el ámbito laboral",
    name_en: "Data protection in the workplace",
    description:
      "Tratamiento de datos de trabajadores, control empresarial, geolocalización y videovigilancia laboral (art. 87-91 LOPDGDD).",
    keywords: [
      "trabajador", "empleado", "laboral", "empresa",
      "relación laboral", "control empresarial",
      "geolocalización", "correo electrónico del empleado",
    ],
  },
  {
    id: "sanidad",
    name_es: "Datos de salud",
    name_en: "Health data",
    description:
      "Tratamiento de datos de salud, historia clínica y datos sanitarios (art. 9 RGPD).",
    keywords: [
      "salud", "sanitario", "médico", "hospital", "clínica",
      "historia clínica", "paciente", "farmacéutico",
      "dato de salud", "categorías especiales",
    ],
  },
  {
    id: "inteligencia_artificial",
    name_es: "Inteligencia artificial",
    name_en: "Artificial intelligence",
    description:
      "Tratamiento de datos personales mediante sistemas de inteligencia artificial, aprendizaje automático y decisiones automatizadas.",
    keywords: [
      "inteligencia artificial", "ia", "machine learning",
      "aprendizaje automático", "algoritmo", "decisión automatizada",
      "perfilado", "elaboración de perfiles", "ia generativa",
    ],
  },
  {
    id: "financiero",
    name_es: "Sector financiero y solvencia",
    name_en: "Financial sector and creditworthiness",
    description:
      "Tratamiento de datos en el sector financiero, ficheros de solvencia y morosidad (art. 20 LOPDGDD).",
    keywords: [
      "banco", "bancario", "financiero", "solvencia",
      "moroso", "morosidad", "crédito", "fichero de morosos",
      "equifax", "asnef", "experian",
    ],
  },
];

// ---------------------------------------------------------------------------
// GDPR/RGPD article detection — extracts article numbers from text
// ---------------------------------------------------------------------------

const GDPR_ARTICLE_PATTERN =
  /\bart(?:[íi]culo|\.)\s*(\d+(?:\s*(?:,\s*\d+|\.\d+|y\s+\d+))*)\s*(?:del\s+)?(?:RGPD|GDPR|LOPDGDD|LOPD|LSSI)?/gi;

function extractGdprArticles(text: string): string[] {
  const articles = new Set<string>();
  let match: RegExpExecArray | null;

  // Reset lastIndex for global pattern reuse
  GDPR_ARTICLE_PATTERN.lastIndex = 0;

  while ((match = GDPR_ARTICLE_PATTERN.exec(text)) !== null) {
    const numStr = match[1];
    if (!numStr) continue;

    // Split compound references: "5, 6 y 13" -> ["5", "6", "13"]
    const nums = numStr.split(/[,\sy]+/).map((s) => s.trim()).filter(Boolean);
    for (const n of nums) {
      const parsed = parseInt(n, 10);
      if (!isNaN(parsed) && parsed >= 1 && parsed <= 99) {
        articles.add(String(parsed));
      }
    }
  }

  // Also match "art. 5.1.a) del RGPD" pattern with paragraph references
  const lidPattern = /\bart(?:[íi]culo|\.)\s*(\d+)(?:\.\d+)?(?:\s*(?:[a-z]\))?)/gi;
  while ((match = lidPattern.exec(text)) !== null) {
    const art = match[1];
    if (art) {
      const parsed = parseInt(art, 10);
      if (!isNaN(parsed) && parsed >= 1 && parsed <= 99) {
        articles.add(String(parsed));
      }
    }
  }

  return [...articles].sort((a, b) => parseInt(a, 10) - parseInt(b, 10));
}

// ---------------------------------------------------------------------------
// Topic detection
// ---------------------------------------------------------------------------

function detectTopics(text: string): string[] {
  const lower = text.toLowerCase();
  const matched: string[] = [];

  for (const rule of TOPIC_RULES) {
    const hit = rule.keywords.some((kw) => lower.includes(kw.toLowerCase()));
    if (hit) {
      matched.push(rule.id);
    }
  }

  return matched;
}

// ---------------------------------------------------------------------------
// Fine amount extraction (Spanish number formats)
// ---------------------------------------------------------------------------

const FINE_PATTERNS = [
  // "6.000.000 euros", "250.000 euros", "8.150.000 euros"
  /(\d{1,3}(?:\.\d{3})*)\s*(?:euros?|EUR)/gi,
  // "EUR 750.000", "EUR 3.700.000"
  /EUR\s*(\d{1,3}(?:\.\d{3})*)/gi,
  // "6.000.000€", "€6.000.000", "€ 6.000.000"
  /\u20ac\s*(\d{1,3}(?:\.\d{3})*)/gi,
  /(\d{1,3}(?:\.\d{3})*)\s*\u20ac/gi,
  // Comma as thousands separator: "6,000,000 euros"
  /(\d{1,3}(?:,\d{3})*)\s*(?:euros?|EUR)/gi,
];

function extractFineAmount(text: string): number | null {
  let maxFine = 0;

  for (const pattern of FINE_PATTERNS) {
    pattern.lastIndex = 0;
    let match: RegExpExecArray | null;
    while ((match = pattern.exec(text)) !== null) {
      const rawNum = match[1];
      if (!rawNum) continue;

      // Parse Spanish-format "3.700.000" or English "3,700,000"
      const normalized = rawNum.replace(/[.,]/g, "");
      const amount = parseInt(normalized, 10);

      // Only count amounts that look like fines (>= 900 EUR and reasonable)
      if (!isNaN(amount) && amount >= 900 && amount <= 100_000_000 && amount > maxFine) {
        maxFine = amount;
      }
    }
  }

  return maxFine > 0 ? maxFine : null;
}

// ---------------------------------------------------------------------------
// Date extraction (Spanish date formats)
// ---------------------------------------------------------------------------

const SPANISH_MONTHS: Record<string, string> = {
  enero: "01", febrero: "02", marzo: "03", abril: "04",
  mayo: "05", junio: "06", julio: "07", agosto: "08",
  septiembre: "09", octubre: "10", noviembre: "11", diciembre: "12",
};

function parseSpanishDate(text: string): string | null {
  // "13 de Marzo de 2026", "27 de enero de 2023"
  const esMatch = text.match(
    /(\d{1,2})\s+de\s+(enero|febrero|marzo|abril|mayo|junio|julio|agosto|septiembre|octubre|noviembre|diciembre)\s+de\s+(\d{4})/i,
  );
  if (esMatch) {
    const day = (esMatch[1] ?? "").padStart(2, "0");
    const month = SPANISH_MONTHS[(esMatch[2] ?? "").toLowerCase()];
    const year = esMatch[3];
    if (month && year) {
      return `${year}-${month}-${day}`;
    }
  }

  // ISO date: "2023-09-13"
  const isoMatch = text.match(/(\d{4}-\d{2}-\d{2})/);
  if (isoMatch) {
    return isoMatch[1] ?? null;
  }

  // "13/01/2023" — DD/MM/YYYY (common Spanish format)
  const slashMatch = text.match(/(\d{2})\/(\d{2})\/(\d{4})/);
  if (slashMatch) {
    const day = slashMatch[1];
    const month = slashMatch[2];
    const year = slashMatch[3];
    if (day && month && year) {
      return `${year}-${month}-${day}`;
    }
  }

  return null;
}

// ---------------------------------------------------------------------------
// Reference extraction from PDF filename or listing text
// ---------------------------------------------------------------------------

/**
 * Extract a reference code from a PDF path or text.
 * Patterns: PS-00120-2022, PD-00044-2026, AI-00385-2024,
 *           REPOSICION-PS-00164-2025, EXP202302620
 */
function extractReference(input: string): string {
  const upper = input.toUpperCase();

  // REPOSICION-PS-00164-2025
  const repoMatch = upper.match(/(REPOSICION-(?:PS|PD|AI|TD)-\d{5}-\d{4})/);
  if (repoMatch) return repoMatch[1]!;

  // PS-00120-2022, PD-00044-2026, AI-00385-2024, TD-00123-2022
  const stdMatch = upper.match(/((?:PS|PD|AI|TD)-\d{5}-\d{4})/);
  if (stdMatch) return stdMatch[1]!;

  // EXP202302620
  const expMatch = upper.match(/(EXP\d{9,})/);
  if (expMatch) return expMatch[1]!;

  // Fallback: generate from slug
  const slug = input.split("/").pop()?.replace(/\.pdf$/i, "") ?? input;
  return `AEPD-${slug.toUpperCase()}`;
}

/**
 * Infer decision type from reference prefix.
 */
function inferDecisionType(reference: string): string {
  const upper = reference.toUpperCase();
  if (upper.startsWith("PS-")) return "sancion";
  if (upper.startsWith("PD-")) return "derechos";
  if (upper.startsWith("AI-")) return "apercibimiento";
  if (upper.startsWith("TD-")) return "tutela";
  if (upper.includes("REPOSICION")) return "reposicion";
  return "resolucion";
}

/**
 * Extract entity name from resolution title or body.
 * Looks for patterns like "... contra EMPRESA S.A. ..." or "... ENTIDAD ..."
 */
function extractEntityName(text: string): string | null {
  // "contra [Entity Name], S.A." / "contra [Entity Name] S.L."
  const contraMatch = text.match(
    /contra\s+(.+?)\s*(?:,\s*(?:S\.?A\.?U?\.?|S\.?L\.?U?\.?|S\.?A\.?)|\s+por\s|\s+en\s+relación|\.\s)/i,
  );
  if (contraMatch && contraMatch[1]) {
    let entity = contraMatch[1].trim();
    // Clean up common suffixes that might be captured
    entity = entity.replace(/\s*,\s*$/, "").trim();
    if (entity.length > 2 && entity.length < 200) {
      return entity;
    }
  }

  // "RESOLUCIÓN ... a [Entity]" pattern at start of sanction text
  const resolucionMatch = text.match(
    /(?:impone|sanciona)\s+a\s+(.+?)\s*(?:,\s*(?:S\.?A\.?U?\.?|S\.?L\.?U?\.?)|una\s+(?:sanción|multa))/i,
  );
  if (resolucionMatch && resolucionMatch[1]) {
    const entity = resolucionMatch[1].trim();
    if (entity.length > 2 && entity.length < 200) {
      return entity;
    }
  }

  return null;
}

// ---------------------------------------------------------------------------
// HTTP fetch with retry, rate limiting, and proper headers
// ---------------------------------------------------------------------------

let lastFetchTime = 0;

async function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function rateLimitedFetch(url: string): Promise<Response | null> {
  const now = Date.now();
  const elapsed = now - lastFetchTime;
  if (elapsed < RATE_LIMIT_MS) {
    await sleep(RATE_LIMIT_MS - elapsed);
  }

  for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
    try {
      lastFetchTime = Date.now();
      const res = await fetch(url, {
        headers: {
          "User-Agent": USER_AGENT,
          Accept: "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
          "Accept-Language": "es-ES,es;q=0.9,en;q=0.5",
        },
        redirect: "follow",
      });

      if (res.ok) {
        return res;
      }

      // 429 Too Many Requests — back off
      if (res.status === 429) {
        const retryAfter = parseInt(res.headers.get("Retry-After") ?? "10", 10);
        console.warn(`  Rate limited (429), waiting ${retryAfter}s before retry ${attempt}/${MAX_RETRIES}`);
        await sleep(retryAfter * 1000);
        continue;
      }

      // 403 Forbidden — skip after 1 attempt
      if (res.status === 403) {
        console.warn(`  Blocked (403): ${url}`);
        return null;
      }

      // 404 Not Found
      if (res.status === 404) {
        console.warn(`  Not found (404): ${url}`);
        return null;
      }

      // Server errors — retry with backoff
      if (res.status >= 500) {
        console.warn(`  Server error (${res.status}), retry ${attempt}/${MAX_RETRIES}: ${url}`);
        await sleep(2000 * attempt);
        continue;
      }

      // Unexpected status
      console.warn(`  HTTP ${res.status} for ${url}`);
      return null;
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      console.warn(`  Network error (attempt ${attempt}/${MAX_RETRIES}): ${msg}`);
      if (attempt < MAX_RETRIES) {
        await sleep(2000 * attempt);
      }
    }
  }

  console.error(`  Failed after ${MAX_RETRIES} retries: ${url}`);
  return null;
}

// ---------------------------------------------------------------------------
// Listing page parsing — discover decision references from Drupal Views
// ---------------------------------------------------------------------------

interface ListingEntry {
  /** Reference code extracted from the entry (e.g. "PS-00120-2022") */
  reference: string;
  /** PDF download path (e.g. "/documento/ps-00120-2022.pdf") */
  pdfPath: string;
  /** Date string extracted from "Fecha de firma" field */
  date: string | null;
  /** Title/header text from the listing entry */
  title: string | null;
}

/**
 * Parse an AEPD resolution listing page.
 *
 * The page uses a Drupal Views structure:
 *   <div class="views-row">
 *     <h3>PS-00211-2025</h3>
 *     <p>1 / 12  Expediente N.º: EXP202514775...</p>
 *     <a href="/documento/ps-00211-2025.pdf">Ver documento</a>
 *     <p>Fecha de firma: 13 de Marzo de 2026</p>
 *   </div>
 */
function parseDecisionListingPage(html: string): ListingEntry[] {
  const $ = cheerio.load(html);
  const entries: ListingEntry[] = [];

  // Find all links to /documento/*.pdf
  $('a[href*="/documento/"]').each((_i: number, el: any) => {
    const href = $(el).attr("href");
    if (!href || !href.endsWith(".pdf")) return;

    const pdfPath = href.startsWith("http")
      ? new URL(href).pathname
      : href.split("?")[0]!;

    const reference = extractReference(pdfPath);

    // Walk up to find the parent container (.views-row or nearest wrapping div)
    const container = $(el).closest(".views-row").length > 0
      ? $(el).closest(".views-row")
      : $(el).parent();

    // Extract date from "Fecha de firma: ..." text
    let date: string | null = null;
    const containerText = container.text();
    const dateMatch = containerText.match(/Fecha\s+de\s+firma:\s*(.+?)(?:\n|$)/i);
    if (dateMatch && dateMatch[1]) {
      date = parseSpanishDate(dateMatch[1].trim());
    }

    // Extract title from heading (h3, h2) within container
    let title: string | null = null;
    const heading = container.find("h3, h2").first();
    if (heading.length > 0) {
      title = heading.text().trim();
    }

    // Avoid duplicates
    if (!entries.some((e) => e.reference === reference)) {
      entries.push({ reference, pdfPath, date, title });
    }
  });

  return entries;
}

/**
 * Parse an AEPD guidelines listing page.
 *
 * Structure:
 *   - Guide entries with title, description, PDF link, publication date
 *   - Pagination via ?page=N (0-indexed)
 */
function parseGuidelineListingPage(html: string): GuidelineSource[] {
  const $ = cheerio.load(html);
  const sources: GuidelineSource[] = [];

  // Find PDF links (could be /guias/*.pdf or /documento/*.pdf)
  $('a[href$=".pdf"]').each((_i: number, el: any) => {
    const href = $(el).attr("href");
    if (!href) return;

    // Only AEPD guides — filter out external links
    if (href.startsWith("http") && !href.includes("aepd.es")) return;

    const url = href.startsWith("http")
      ? new URL(href).pathname
      : href.split("?")[0]!;

    // Skip if it is a decision PDF (contains ps-, pd-, ai- reference patterns)
    if (/\/(ps|pd|ai|td)-\d{5}-\d{4}\.pdf/i.test(url)) return;

    // Walk up to find container
    const container = $(el).parent();
    const containerText = container.text() + " " + container.parent().text();

    // Extract title — look at the closest heading or the link text
    let title: string | null = null;
    const heading = container.prevAll("h3, h2").first();
    if (heading.length > 0) {
      title = heading.text().trim();
    }
    if (!title) {
      // Use the link text itself (often contains the title + file size)
      const linkText = $(el).text().trim();
      // Strip file size info: "Guide Title (2.17 MB)" -> "Guide Title"
      title = linkText.replace(/\s*\(\d+(?:\.\d+)?\s*(?:MB|KB|bytes?)\)\s*$/i, "").trim();
    }

    // Extract date
    let date: string | null = null;
    const dateCandidate = parseSpanishDate(containerText);
    if (dateCandidate) {
      date = dateCandidate;
    }

    const reference = extractReference(url);

    if (title && title.length > 3 && !sources.some((s) => s.url === url)) {
      sources.push({
        url,
        reference,
        title,
        date: date ?? undefined,
        type: "guia",
      });
    }
  });

  return sources;
}

/**
 * Check if a listing page has a "next" page link.
 * AEPD uses .pager with ?page=N links.
 */
function getNextPageUrl(html: string, currentPage: number): string | null {
  const $ = cheerio.load(html);

  // Look for pagination links
  const nextPage = currentPage + 1;

  // Check for a link with the next page number
  let nextUrl: string | null = null;

  $(".pager a, .pagination a, nav a").each((_i: number, el: any) => {
    const href = $(el).attr("href");
    if (!href) return;

    if (href.includes(`page=${nextPage}`)) {
      nextUrl = href;
    }
  });

  return nextUrl;
}

// ---------------------------------------------------------------------------
// PDF text extraction fallback
//
// AEPD resolution PDFs are the primary source. When we can only get the PDF
// (no HTML detail page), we extract text from the first N bytes to get the
// resolution header (expediente, entity, etc.). This is a lightweight
// extraction that does not require a full PDF parser — it scans the raw
// PDF stream for readable text fragments in the header pages.
// ---------------------------------------------------------------------------

/**
 * Extract readable text fragments from a PDF binary buffer.
 * This is a minimal approach: scans for text between parentheses in the
 * PDF content stream, which captures most Spanish text in LibreOffice-
 * generated PDFs used by AEPD.
 *
 * Returns concatenated text fragments, or null if extraction fails.
 */
function extractTextFromPdfBuffer(buffer: ArrayBuffer): string | null {
  const bytes = new Uint8Array(buffer);

  // Decode as Latin-1 (ISO 8859-1) to handle Spanish characters in PDF streams
  let raw: string;
  try {
    raw = new TextDecoder("latin1").decode(bytes);
  } catch {
    return null;
  }

  const fragments: string[] = [];

  // Extract text between parentheses in PDF content streams: (Text here) Tj
  const textMatches = raw.matchAll(/\(([^)]{2,})\)\s*Tj/g);
  for (const m of textMatches) {
    if (m[1]) {
      // Unescape PDF string escapes: \n, \r, \\, \(, \)
      const unescaped = m[1]
        .replace(/\\n/g, "\n")
        .replace(/\\r/g, "\r")
        .replace(/\\\\/g, "\\")
        .replace(/\\\(/g, "(")
        .replace(/\\\)/g, ")");
      fragments.push(unescaped);
    }
  }

  // Also try BT ... ET blocks with TJ arrays: [(text) -kern (text)] TJ
  const tjMatches = raw.matchAll(/\[([^\]]+)\]\s*TJ/g);
  for (const m of tjMatches) {
    if (!m[1]) continue;
    const innerTexts = m[1].matchAll(/\(([^)]+)\)/g);
    for (const inner of innerTexts) {
      if (inner[1] && inner[1].length > 1) {
        fragments.push(inner[1]);
      }
    }
  }

  if (fragments.length === 0) return null;

  // Join and normalize whitespace
  return fragments.join(" ").replace(/\s+/g, " ").trim();
}

// ---------------------------------------------------------------------------
// Database operations
// ---------------------------------------------------------------------------

function initDb(): Database.Database {
  const dir = dirname(DB_PATH);
  if (!existsSync(dir)) {
    mkdirSync(dir, { recursive: true });
    console.log(`Created data directory: ${dir}`);
  }

  if (FLAG_FORCE && existsSync(DB_PATH)) {
    unlinkSync(DB_PATH);
    console.log(`Deleted existing database (--force)`);
  }

  const db = new Database(DB_PATH);
  db.pragma("journal_mode = WAL");
  db.pragma("foreign_keys = ON");
  db.exec(SCHEMA_SQL);

  return db;
}

function getExistingDecisionRefs(db: Database.Database): Set<string> {
  const refs = new Set<string>();
  const rows = db.prepare("SELECT reference FROM decisions").all() as Array<{ reference: string }>;
  for (const row of rows) {
    refs.add(row.reference);
  }
  return refs;
}

function getExistingGuidelineRefs(db: Database.Database): Set<string> {
  const refs = new Set<string>();
  const rows = db
    .prepare("SELECT reference FROM guidelines WHERE reference IS NOT NULL")
    .all() as Array<{ reference: string }>;
  for (const row of rows) {
    refs.add(row.reference);
  }
  return refs;
}

// ---------------------------------------------------------------------------
// Ingestion stats
// ---------------------------------------------------------------------------

interface IngestStats {
  decisionsIngested: number;
  decisionsSkipped: number;
  decisionsFailed: number;
  guidelinesIngested: number;
  guidelinesSkipped: number;
  guidelinesFailed: number;
  pagesScanned: number;
  discoveredDecisions: number;
  discoveredGuidelines: number;
}

// ---------------------------------------------------------------------------
// Decision ingestion — fetch PDF, extract metadata, insert to DB
// ---------------------------------------------------------------------------

async function ingestDecision(
  db: Database.Database,
  source: DecisionSource,
  existingRefs: Set<string>,
  stats: IngestStats,
): Promise<void> {
  const reference = source.reference ?? extractReference(source.pdfPath);

  if (FLAG_RESUME && existingRefs.has(reference)) {
    console.log(`  [skip] ${reference} (already in DB)`);
    stats.decisionsSkipped++;
    return;
  }

  const fullUrl = source.pdfPath.startsWith("http")
    ? source.pdfPath
    : `${BASE_URL}${source.pdfPath}`;
  console.log(`  Fetching: ${fullUrl}`);

  const res = await rateLimitedFetch(fullUrl);
  if (!res) {
    stats.decisionsFailed++;
    return;
  }

  // The response is a PDF — extract text from the binary content
  let bodyText: string;
  const contentType = res.headers.get("content-type") ?? "";

  if (contentType.includes("pdf")) {
    const buffer = await res.arrayBuffer();
    const extracted = extractTextFromPdfBuffer(buffer);

    if (!extracted || extracted.length < 50) {
      console.warn(`  Could not extract sufficient text from PDF: ${reference}`);
      // Use title and reference as minimal content
      bodyText = source.title ?? `Resolución ${reference} de la Agencia Española de Protección de Datos.`;
    } else {
      bodyText = extracted;
    }
  } else if (contentType.includes("html")) {
    // Some URLs may redirect to HTML pages
    const html = await res.text();
    const $ = cheerio.load(html);
    $("nav, header, footer, script, style, .breadcrumb, .pager, .sidebar, .menu").remove();
    bodyText = $("main").text().replace(/\s+/g, " ").trim() ||
      $("body").text().replace(/\s+/g, " ").trim();

    if (bodyText.length < 50) {
      console.warn(`  Body text too short (${bodyText.length} chars) for: ${reference}`);
      bodyText = source.title ?? `Resolución ${reference} de la Agencia Española de Protección de Datos.`;
    }
  } else {
    console.warn(`  Unexpected content type (${contentType}) for: ${fullUrl}`);
    stats.decisionsFailed++;
    return;
  }

  // Build structured decision
  const type = source.type ?? inferDecisionType(reference);
  const title = source.title ??
    `Resolución AEPD — ${reference}`;
  const date = source.date ?? parseSpanishDate(bodyText);
  const entityName = extractEntityName(bodyText);
  const fineAmount = type === "sancion" ? extractFineAmount(bodyText) : null;
  const topics = detectTopics(`${title} ${bodyText}`);
  const gdprArticles = extractGdprArticles(bodyText);

  // Generate a summary from the first meaningful paragraph (max 500 chars)
  let summary: string | null = null;
  if (bodyText.length > 200) {
    // Find the first sentence after "RESUELVE" or "HECHOS PROBADOS" if present
    const resuelveIdx = bodyText.search(/(?:RESUELVE|PRIMERO|HECHOS\s+PROBADOS)/i);
    const summaryStart = resuelveIdx >= 0 ? resuelveIdx : 0;
    const summarySlice = bodyText.slice(summaryStart, summaryStart + 500);
    summary = summarySlice.replace(/\s+/g, " ").trim();
    if (summary.length > 490) {
      summary = summary.slice(0, summary.lastIndexOf(" ")) + "...";
    }
  }

  if (FLAG_DRY_RUN) {
    console.log(`  [dry-run] Would insert decision: ${reference}`);
    console.log(`    Title: ${title}`);
    console.log(`    Date: ${date ?? "unknown"}`);
    console.log(`    Entity: ${entityName ?? "unknown"}`);
    console.log(`    Fine: ${fineAmount != null ? `${fineAmount.toLocaleString("es-ES")} EUR` : "N/A"}`);
    console.log(`    Type: ${type}`);
    console.log(`    Topics: ${topics.join(", ") || "none detected"}`);
    console.log(`    GDPR articles: ${gdprArticles.join(", ") || "none detected"}`);
    console.log(`    Body length: ${bodyText.length} chars`);
    stats.decisionsIngested++;
    return;
  }

  try {
    db.prepare(`
      INSERT OR REPLACE INTO decisions
        (reference, title, date, type, entity_name, fine_amount, summary, full_text, topics, gdpr_articles, status)
      VALUES
        (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `).run(
      reference,
      title,
      date,
      type,
      entityName,
      fineAmount,
      summary,
      bodyText,
      JSON.stringify(topics),
      JSON.stringify(gdprArticles),
      "final",
    );
    console.log(`  [ok] Inserted decision: ${reference}`);
    stats.decisionsIngested++;
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    console.error(`  [error] Failed to insert ${reference}: ${msg}`);
    stats.decisionsFailed++;
  }
}

// ---------------------------------------------------------------------------
// Guideline ingestion — fetch PDF or HTML page, extract content, insert
// ---------------------------------------------------------------------------

async function ingestGuideline(
  db: Database.Database,
  source: GuidelineSource,
  existingRefs: Set<string>,
  stats: IngestStats,
): Promise<void> {
  const reference = source.reference ?? extractReference(source.url);

  if (FLAG_RESUME && existingRefs.has(reference)) {
    console.log(`  [skip] ${reference} (already in DB)`);
    stats.guidelinesSkipped++;
    return;
  }

  const fullUrl = source.url.startsWith("http")
    ? source.url
    : `${BASE_URL}${source.url}`;
  console.log(`  Fetching: ${fullUrl}`);

  const res = await rateLimitedFetch(fullUrl);
  if (!res) {
    stats.guidelinesFailed++;
    return;
  }

  let bodyText: string;
  const contentType = res.headers.get("content-type") ?? "";

  if (contentType.includes("pdf")) {
    const buffer = await res.arrayBuffer();
    const extracted = extractTextFromPdfBuffer(buffer);

    if (!extracted || extracted.length < 50) {
      console.warn(`  Could not extract sufficient text from PDF: ${reference}`);
      bodyText = source.title ?? `Guía AEPD — ${reference}`;
    } else {
      bodyText = extracted;
    }
  } else if (contentType.includes("html")) {
    const html = await res.text();
    const $ = cheerio.load(html);
    $("nav, header, footer, script, style, .breadcrumb, .pager, .sidebar, .menu").remove();

    bodyText =
      $(".field--name-body").text().replace(/\s+/g, " ").trim() ||
      $("article").text().replace(/\s+/g, " ").trim() ||
      $("main").text().replace(/\s+/g, " ").trim() ||
      $("body").text().replace(/\s+/g, " ").trim();

    if (bodyText.length < 50) {
      console.warn(`  Body text too short (${bodyText.length} chars) for: ${reference}`);
      bodyText = source.title ?? `Guía AEPD — ${reference}`;
    }
  } else {
    console.warn(`  Unexpected content type (${contentType}) for: ${fullUrl}`);
    stats.guidelinesFailed++;
    return;
  }

  const title = source.title ?? `Guía AEPD — ${reference}`;
  const date = source.date ?? parseSpanishDate(bodyText);
  const type = source.type ?? "guia";
  const topics = detectTopics(`${title} ${bodyText}`);

  // Generate summary from first 500 chars of body
  let summary: string | null = null;
  if (bodyText.length > 100) {
    const summarySlice = bodyText.slice(0, 500);
    summary = summarySlice.replace(/\s+/g, " ").trim();
    if (summary.length > 490) {
      summary = summary.slice(0, summary.lastIndexOf(" ")) + "...";
    }
  }

  if (FLAG_DRY_RUN) {
    console.log(`  [dry-run] Would insert guideline: ${reference}`);
    console.log(`    Title: ${title}`);
    console.log(`    Date: ${date ?? "unknown"}`);
    console.log(`    Type: ${type}`);
    console.log(`    Topics: ${topics.join(", ") || "none detected"}`);
    console.log(`    Body length: ${bodyText.length} chars`);
    stats.guidelinesIngested++;
    return;
  }

  try {
    db.prepare(`
      INSERT OR REPLACE INTO guidelines
        (reference, title, date, type, summary, full_text, topics, language)
      VALUES
        (?, ?, ?, ?, ?, ?, ?, ?)
    `).run(
      reference,
      title,
      date,
      type,
      summary,
      bodyText,
      JSON.stringify(topics),
      "es",
    );
    console.log(`  [ok] Inserted guideline: ${reference}`);
    stats.guidelinesIngested++;
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    console.error(`  [error] Failed to insert guideline ${reference}: ${msg}`);
    stats.guidelinesFailed++;
  }
}

// ---------------------------------------------------------------------------
// Ensure topics exist in database
// ---------------------------------------------------------------------------

function ensureTopics(db: Database.Database): void {
  const insertTopic = db.prepare(
    "INSERT OR IGNORE INTO topics (id, name_es, name_en, description) VALUES (?, ?, ?, ?)",
  );

  const insertAll = db.transaction(() => {
    for (const rule of TOPIC_RULES) {
      insertTopic.run(rule.id, rule.name_es, rule.name_en, rule.description);
    }
  });

  insertAll();
}

// ---------------------------------------------------------------------------
// Listing page crawling — paginated discovery of decisions and guidelines
// ---------------------------------------------------------------------------

async function crawlDecisionListingPages(
  existingRefs: Set<string>,
): Promise<DecisionSource[]> {
  const discovered: DecisionSource[] = [];
  const seenRefs = new Set<string>();

  // Add known refs to avoid duplicating curated entries
  for (const d of KNOWN_DECISIONS) {
    const ref = d.reference ?? extractReference(d.pdfPath);
    seenRefs.add(ref);
  }

  console.log(`Crawling decision listing pages (max ${MAX_PAGES} pages)...`);

  for (let page = 0; page < MAX_PAGES; page++) {
    const pageUrl = page === 0
      ? `${BASE_URL}${DECISION_LIST_BASE}`
      : `${BASE_URL}${DECISION_LIST_BASE}?page=${page}`;

    console.log(`  Page ${page + 1}: ${pageUrl}`);
    const res = await rateLimitedFetch(pageUrl);
    if (!res) {
      console.log(`  Listing page unavailable at page ${page + 1}, stopping`);
      break;
    }

    const html = await res.text();
    const entries = parseDecisionListingPage(html);

    if (entries.length === 0) {
      console.log(`  No entries found on page ${page + 1}, stopping`);
      break;
    }

    let newOnThisPage = 0;
    for (const entry of entries) {
      if (seenRefs.has(entry.reference)) continue;
      seenRefs.add(entry.reference);

      // In resume mode, skip already-ingested references during discovery too
      if (FLAG_RESUME && existingRefs.has(entry.reference)) {
        continue;
      }

      discovered.push({
        pdfPath: entry.pdfPath,
        reference: entry.reference,
        type: inferDecisionType(entry.reference) as DecisionSource["type"],
        date: entry.date ?? undefined,
        title: entry.title ?? undefined,
      });
      newOnThisPage++;
    }

    console.log(`    Found ${entries.length} entries (${newOnThisPage} new)`);

    // Check if there is a next page
    if (getNextPageUrl(html, page) === null) {
      console.log(`  No more pages after ${page + 1}`);
      break;
    }
  }

  return discovered;
}

async function crawlGuidelineListingPages(
  existingRefs: Set<string>,
): Promise<GuidelineSource[]> {
  const discovered: GuidelineSource[] = [];
  const seenUrls = new Set<string>();

  // Mark known guide URLs to avoid duplicating curated entries
  for (const g of KNOWN_GUIDELINES) {
    seenUrls.add(g.url);
  }

  console.log(`Crawling guideline listing pages (max ${MAX_PAGES} pages)...`);

  for (let page = 0; page < MAX_PAGES; page++) {
    const pageUrl = page === 0
      ? `${BASE_URL}${GUIDELINE_LIST_BASE}`
      : `${BASE_URL}${GUIDELINE_LIST_BASE}?page=${page}`;

    console.log(`  Page ${page + 1}: ${pageUrl}`);
    const res = await rateLimitedFetch(pageUrl);
    if (!res) {
      console.log(`  Listing page unavailable at page ${page + 1}, stopping`);
      break;
    }

    const html = await res.text();
    const entries = parseGuidelineListingPage(html);

    if (entries.length === 0) {
      console.log(`  No entries found on page ${page + 1}, stopping`);
      break;
    }

    let newOnThisPage = 0;
    for (const entry of entries) {
      if (seenUrls.has(entry.url)) continue;
      seenUrls.add(entry.url);

      // In resume mode, skip already-ingested references
      const ref = entry.reference ?? extractReference(entry.url);
      if (FLAG_RESUME && existingRefs.has(ref)) {
        continue;
      }

      discovered.push(entry);
      newOnThisPage++;
    }

    console.log(`    Found ${entries.length} entries (${newOnThisPage} new)`);

    // Check if there is a next page
    if (getNextPageUrl(html, page) === null) {
      console.log(`  No more pages after ${page + 1}`);
      break;
    }
  }

  return discovered;
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main(): Promise<void> {
  console.log("=== AEPD (Agencia Española de Protección de Datos) Ingestion Crawler ===");
  console.log();
  console.log(`Database:    ${DB_PATH}`);
  console.log(`Rate limit:  ${RATE_LIMIT_MS}ms between requests`);
  console.log(`Max retries: ${MAX_RETRIES}`);
  console.log(`Max pages:   ${MAX_PAGES} per listing source`);
  console.log(`Flags:       ${[
    FLAG_RESUME && "--resume",
    FLAG_DRY_RUN && "--dry-run",
    FLAG_FORCE && "--force",
  ].filter(Boolean).join(" ") || "(none)"}`);
  console.log();

  // -- Init database --------------------------------------------------------
  const db = initDb();

  ensureTopics(db);
  console.log(`Ensured ${TOPIC_RULES.length} topics in database`);

  const existingDecisionRefs = getExistingDecisionRefs(db);
  const existingGuidelineRefs = getExistingGuidelineRefs(db);

  if (FLAG_RESUME) {
    console.log(`Existing decisions: ${existingDecisionRefs.size}`);
    console.log(`Existing guidelines: ${existingGuidelineRefs.size}`);
  }

  const stats: IngestStats = {
    decisionsIngested: 0,
    decisionsSkipped: 0,
    decisionsFailed: 0,
    guidelinesIngested: 0,
    guidelinesSkipped: 0,
    guidelinesFailed: 0,
    pagesScanned: 0,
    discoveredDecisions: 0,
    discoveredGuidelines: 0,
  };

  // -- Phase 1: Discover decisions from listing pages -----------------------
  console.log();
  console.log("--- Phase 1: Discover decisions from listing pages ---");

  const discoveredDecisions = await crawlDecisionListingPages(existingDecisionRefs);
  stats.discoveredDecisions = discoveredDecisions.length;

  // Merge discovered with curated (curated takes precedence for metadata)
  const curatedPdfPaths = new Set(KNOWN_DECISIONS.map((d) => d.pdfPath));
  const allDecisionSources: DecisionSource[] = [...KNOWN_DECISIONS];

  for (const d of discoveredDecisions) {
    if (!curatedPdfPaths.has(d.pdfPath)) {
      allDecisionSources.push(d);
    }
  }

  console.log(`Total decision sources: ${allDecisionSources.length} (${KNOWN_DECISIONS.length} curated + ${discoveredDecisions.length} discovered)`);

  // -- Phase 2: Discover guidelines from listing pages ----------------------
  console.log();
  console.log("--- Phase 2: Discover guidelines from listing pages ---");

  const discoveredGuidelines = await crawlGuidelineListingPages(existingGuidelineRefs);
  stats.discoveredGuidelines = discoveredGuidelines.length;

  const curatedGuideUrls = new Set(KNOWN_GUIDELINES.map((g) => g.url));
  const allGuidelineSources: GuidelineSource[] = [...KNOWN_GUIDELINES];

  for (const g of discoveredGuidelines) {
    if (!curatedGuideUrls.has(g.url)) {
      allGuidelineSources.push(g);
    }
  }

  console.log(`Total guideline sources: ${allGuidelineSources.length} (${KNOWN_GUIDELINES.length} curated + ${discoveredGuidelines.length} discovered)`);

  // -- Phase 3: Ingest decisions --------------------------------------------
  console.log();
  console.log("--- Phase 3: Ingesting decisions ---");

  for (const source of allDecisionSources) {
    await ingestDecision(db, source, existingDecisionRefs, stats);
  }

  // -- Phase 4: Ingest guidelines -------------------------------------------
  console.log();
  console.log("--- Phase 4: Ingesting guidelines ---");

  for (const source of allGuidelineSources) {
    await ingestGuideline(db, source, existingGuidelineRefs, stats);
  }

  // -- Summary --------------------------------------------------------------
  console.log();
  console.log("=== Ingestion Complete ===");
  console.log();
  console.log(`Discovery:`);
  console.log(`  Decisions discovered:  ${stats.discoveredDecisions}`);
  console.log(`  Guidelines discovered: ${stats.discoveredGuidelines}`);
  console.log();
  console.log(`Decisions:`);
  console.log(`  Ingested: ${stats.decisionsIngested}`);
  console.log(`  Skipped:  ${stats.decisionsSkipped}`);
  console.log(`  Failed:   ${stats.decisionsFailed}`);
  console.log();
  console.log(`Guidelines:`);
  console.log(`  Ingested: ${stats.guidelinesIngested}`);
  console.log(`  Skipped:  ${stats.guidelinesSkipped}`);
  console.log(`  Failed:   ${stats.guidelinesFailed}`);

  // -- DB totals -----------------------------------------------------------
  if (!FLAG_DRY_RUN) {
    const decisionCount = (
      db.prepare("SELECT count(*) as cnt FROM decisions").get() as { cnt: number }
    ).cnt;
    const guidelineCount = (
      db.prepare("SELECT count(*) as cnt FROM guidelines").get() as { cnt: number }
    ).cnt;
    const topicCount = (
      db.prepare("SELECT count(*) as cnt FROM topics").get() as { cnt: number }
    ).cnt;

    console.log();
    console.log(`Database totals:`);
    console.log(`  Topics:     ${topicCount}`);
    console.log(`  Decisions:  ${decisionCount}`);
    console.log(`  Guidelines: ${guidelineCount}`);
  }

  console.log();
  console.log(`Done. Database at ${DB_PATH}`);

  db.close();
}

// ---------------------------------------------------------------------------
// Run
// ---------------------------------------------------------------------------

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
