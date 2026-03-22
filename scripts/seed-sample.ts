/**
 * Seed the AEPD (Agencia Española de Protección de Datos) database with sample decisions and guidelines.
 *
 * Includes real AEPD decisions (CaixaBank, Vodafone, La Liga, BBVA, Equifax)
 * and representative guidance documents so MCP tools can be tested without
 * running a full ingestion pipeline.
 *
 * Usage:
 *   npx tsx scripts/seed-sample.ts
 *   npx tsx scripts/seed-sample.ts --force   # drop and recreate
 */

import Database from "better-sqlite3";
import { existsSync, mkdirSync, unlinkSync } from "node:fs";
import { dirname } from "node:path";
import { SCHEMA_SQL } from "../src/db.js";

const DB_PATH = process.env["AEPD_DB_PATH"] ?? "data/aepd.db";
const force = process.argv.includes("--force");

// --- Bootstrap database ------------------------------------------------------

const dir = dirname(DB_PATH);
if (!existsSync(dir)) {
  mkdirSync(dir, { recursive: true });
}

if (force && existsSync(DB_PATH)) {
  unlinkSync(DB_PATH);
  console.log(`Deleted existing database at ${DB_PATH}`);
}

const db = new Database(DB_PATH);
db.pragma("journal_mode = WAL");
db.pragma("foreign_keys = ON");
db.exec(SCHEMA_SQL);

console.log(`Database initialised at ${DB_PATH}`);

// --- Topics ------------------------------------------------------------------

interface TopicRow {
  id: string;
  name_es: string;
  name_en: string;
  description: string;
}

const topics: TopicRow[] = [
  {
    id: "videovigilancia",
    name_es: "Videovigilancia y CCTV",
    name_en: "Video surveillance and CCTV",
    description: "Uso de sistemas de videovigilancia en espacios públicos, lugares de trabajo y establecimientos comerciales (art. 22 LOPDGDD).",
  },
  {
    id: "cookies",
    name_es: "Cookies y rastreadores",
    name_en: "Cookies and trackers",
    description: "Instalación y uso de cookies y tecnologías de rastreo en dispositivos de usuarios (art. 22 LSSI y RGPD).",
  },
  {
    id: "telecomunicaciones",
    name_es: "Telecomunicaciones y comunicaciones electrónicas",
    name_en: "Telecommunications and electronic communications",
    description: "Protección de datos en el sector de las telecomunicaciones, incluyendo operadores móviles y proveedores de servicios de internet.",
  },
  {
    id: "publicidad",
    name_es: "Publicidad y marketing directo",
    name_en: "Advertising and direct marketing",
    description: "Comunicaciones comerciales y publicidad personalizada, incluyendo el envío de comunicaciones electrónicas sin consentimiento.",
  },
  {
    id: "derechos_interesados",
    name_es: "Derechos de los interesados",
    name_en: "Data subject rights",
    description: "Ejercicio de los derechos de acceso, rectificación, supresión, limitación, portabilidad y oposición (art. 15-22 RGPD).",
  },
  {
    id: "evaluacion_impacto",
    name_es: "Evaluación de Impacto sobre la Protección de Datos (EIPD)",
    name_en: "Data Protection Impact Assessment (DPIA)",
    description: "Evaluación de riesgos para los derechos y libertades de los interesados en tratamientos de alto riesgo (art. 35 RGPD).",
  },
  {
    id: "transferencias",
    name_es: "Transferencias internacionales",
    name_en: "International transfers",
    description: "Transferencias de datos personales a terceros países u organizaciones internacionales (art. 44-49 RGPD).",
  },
  {
    id: "consentimiento",
    name_es: "Consentimiento",
    name_en: "Consent",
    description: "Validez del consentimiento como base legal para el tratamiento de datos, incluyendo el consentimiento libre, específico, informado e inequívoco (art. 6 y 7 RGPD).",
  },
  {
    id: "menores",
    name_es: "Menores de edad",
    name_en: "Minors",
    description: "Protección de datos de menores de edad en servicios de la sociedad de la información (art. 8 RGPD y art. 7 LOPDGDD — mayoría de edad digital a los 14 años en España).",
  },
];

const insertTopic = db.prepare(
  "INSERT OR IGNORE INTO topics (id, name_es, name_en, description) VALUES (?, ?, ?, ?)",
);

for (const t of topics) {
  insertTopic.run(t.id, t.name_es, t.name_en, t.description);
}

console.log(`Inserted ${topics.length} topics`);

// --- Decisions ---------------------------------------------------------------

interface DecisionRow {
  reference: string;
  title: string;
  date: string;
  type: string;
  entity_name: string;
  fine_amount: number | null;
  summary: string;
  full_text: string;
  topics: string;
  gdpr_articles: string;
  status: string;
}

const decisions: DecisionRow[] = [
  // PS-00120-2022 — CaixaBank (EUR 6M)
  {
    reference: "PS-00120-2022",
    title: "Resolución AEPD — CaixaBank (comunicaciones comerciales y segmentación)",
    date: "2023-01-27",
    type: "sancion",
    entity_name: "CaixaBank, S.A.",
    fine_amount: 6_000_000,
    summary:
      "La AEPD impuso a CaixaBank una sanción de 6 millones de euros por infracciones del RGPD relacionadas con el tratamiento de datos personales de clientes para comunicaciones comerciales y segmentación sin base legal adecuada. La entidad bancaria utilizaba datos de clientes para enviar comunicaciones de marketing sin contar con el consentimiento válido o el interés legítimo debidamente acreditado.",
    full_text:
      "La Agencia Española de Protección de Datos ha impuesto a CaixaBank, S.A. una sanción de 6.000.000 euros por infracciones del Reglamento General de Protección de Datos. El procedimiento sancionador se inició a raíz de reclamaciones de clientes que denunciaron haber recibido comunicaciones comerciales no deseadas por parte de CaixaBank. La AEPD constató las siguientes infracciones: (1) Tratamiento de datos personales de clientes para comunicaciones comerciales sin base legal adecuada (art. 6 RGPD). CaixaBank utilizaba los datos de sus clientes para el envío de comunicaciones de marketing sin haber obtenido el consentimiento válido de los interesados y sin que quedara debidamente acreditado el interés legítimo alegado; (2) Segmentación de clientes para la personalización de ofertas comerciales sin informar adecuadamente a los interesados sobre este tratamiento (art. 5.1.a y 13 RGPD); (3) Dificultades en el ejercicio del derecho de oposición al tratamiento de datos para comunicaciones comerciales (art. 21 RGPD). La AEPD impuso la sanción teniendo en cuenta la naturaleza, gravedad y duración de la infracción, el número de interesados afectados (miles de clientes), la condición de responsable del tratamiento como entidad bancaria de gran tamaño, y las medidas adoptadas para mitigar el daño. CaixaBank alegó haber modificado sus procesos de comunicación comercial durante el procedimiento, lo que fue valorado positivamente aunque no excluyó la sanción.",
    topics: JSON.stringify(["publicidad", "consentimiento", "derechos_interesados"]),
    gdpr_articles: JSON.stringify(["5", "6", "13", "21"]),
    status: "final",
  },
  // PS-00210-2022 — Vodafone España (EUR 8.15M)
  {
    reference: "PS-00210-2022",
    title: "Resolución AEPD — Vodafone España (comunicaciones comerciales y datos de clientes)",
    date: "2022-12-21",
    type: "sancion",
    entity_name: "Vodafone España, S.A.U.",
    fine_amount: 8_150_000,
    summary:
      "La AEPD impuso a Vodafone España una sanción total de 8,15 millones de euros por múltiples infracciones del RGPD, incluida la realización de comunicaciones comerciales fraudulentas (contrataciones no solicitadas), la falta de respuesta adecuada a las reclamaciones de los interesados, y deficiencias en el tratamiento de datos de clientes.",
    full_text:
      "La Agencia Española de Protección de Datos ha resuelto el procedimiento sancionador iniciado contra Vodafone España, S.A.U. y ha impuesto sanciones por un total de 8.150.000 euros por diversas infracciones del RGPD. El procedimiento sancionador se inició como consecuencia de las numerosas reclamaciones recibidas de usuarios afectados por las prácticas comerciales de Vodafone. La AEPD constató las siguientes infracciones principales: (1) Contrataciones no solicitadas: Vodafone realizó contratos de servicios de telecomunicaciones en nombre de personas que no habían dado su consentimiento, utilizando datos personales de terceros de forma fraudulenta. Esto constituye una infracción del art. 6 RGPD (falta de base legal para el tratamiento); (2) Comunicaciones comerciales no deseadas: Vodafone envió comunicaciones comerciales a personas que habían ejercido su derecho de oposición al tratamiento de sus datos para marketing, incumpliendo el art. 21 RGPD; (3) Atención deficiente a reclamaciones: Vodafone no atendió adecuadamente las reclamaciones y solicitudes de ejercicio de derechos de los interesados dentro de los plazos establecidos, incumpliendo los arts. 12 y 17 RGPD; (4) Registro de actividades de tratamiento deficiente: las actividades de tratamiento no estaban correctamente documentadas conforme al art. 30 RGPD. La AEPD impuso la máxima sanción posible por algunas de las infracciones, dado el elevado número de personas afectadas y la reincidencia de la entidad.",
    topics: JSON.stringify(["telecomunicaciones", "publicidad", "consentimiento", "derechos_interesados"]),
    gdpr_articles: JSON.stringify(["6", "12", "17", "21", "30"]),
    status: "final",
  },
  // PS-00131-2019 — La Liga (micrófono app)
  {
    reference: "PS-00131-2019",
    title: "Resolución AEPD — Liga Nacional de Fútbol Profesional (app con micrófono)",
    date: "2019-06-11",
    type: "sancion",
    entity_name: "Liga Nacional de Fútbol Profesional (La Liga)",
    fine_amount: 250_000,
    summary:
      "La AEPD impuso a La Liga una sanción de 250.000 euros por activar el micrófono de los teléfonos móviles de los usuarios de su aplicación oficial para detectar retransmisiones no autorizadas de partidos de fútbol. Los usuarios no eran informados de forma suficientemente clara sobre esta funcionalidad.",
    full_text:
      "La Agencia Española de Protección de Datos ha impuesto a la Liga Nacional de Fútbol Profesional (La Liga) una sanción de 250.000 euros por infracción del art. 13 del RGPD (información que debe facilitarse cuando los datos personales se obtengan del interesado). La AEPD investigó la aplicación oficial de La Liga, que utilizaba el micrófono del dispositivo móvil de los usuarios para captar el audio del entorno, con el fin de detectar los bares y establecimientos que retransmitían partidos de fútbol sin haber contratado los derechos correspondientes. La AEPD constató que, aunque La Liga informaba en sus términos y condiciones sobre la activación del micrófono, esta información no era suficientemente clara, precisa y comprensible para un usuario medio. La información sobre esta funcionalidad estaba incluida en textos largos de condiciones de uso, sin llamar la atención del usuario de forma adecuada sobre una funcionalidad tan intrusiva como la activación del micrófono. Además, la aplicación no solicitaba permisos de forma separada para la activación del micrófono con fines de detección de fraude, diferenciándolos de los permisos necesarios para el funcionamiento ordinario de la aplicación. La AEPD consideró que el tratamiento de datos de audio capturados mediante el micrófono constituye un tratamiento de datos personales que requiere una información específica y clara al usuario, así como una base legal adecuada. La Liga modificó la aplicación para incluir información más clara y solicitó el consentimiento de los usuarios de forma explícita.",
    topics: JSON.stringify(["consentimiento", "publicidad"]),
    gdpr_articles: JSON.stringify(["13", "6"]),
    status: "final",
  },
  // PS-00081-2021 — BBVA (transferencias internacionales)
  {
    reference: "PS-00081-2021",
    title: "Resolución AEPD — Banco Bilbao Vizcaya Argentaria (transferencias internacionales)",
    date: "2021-04-29",
    type: "sancion",
    entity_name: "Banco Bilbao Vizcaya Argentaria, S.A. (BBVA)",
    fine_amount: 5_000_000,
    summary:
      "La AEPD impuso a BBVA una sanción de 5 millones de euros por tratamiento de datos de clientes sin consentimiento adecuado para comunicaciones comerciales y por la cesión de datos a terceros para análisis de solvencia sin cumplir con los requisitos legales aplicables.",
    full_text:
      "La Agencia Española de Protección de Datos ha impuesto al Banco Bilbao Vizcaya Argentaria, S.A. (BBVA) una sanción total de 5.000.000 euros tras el procedimiento sancionador iniciado por diversas infracciones del RGPD. Las principales infracciones detectadas fueron: (1) Tratamiento de datos de clientes para comunicaciones comerciales sin base legal válida: BBVA utilizaba datos de sus clientes para el envío de comunicaciones de marketing sin haber obtenido el consentimiento libre, específico, informado e inequívoco de los interesados, tal como exige el art. 7 RGPD. El banco alegaba la existencia de interés legítimo, pero la AEPD consideró que no se había realizado el test de ponderación adecuado ni se habían informado suficientemente a los clientes sobre este tratamiento; (2) Cesión de datos a sociedades del grupo BBVA y a terceros para análisis de riesgo y solvencia sin contar con la base legal adecuada, incumpliendo el art. 6 RGPD y los arts. 44 y siguientes RGPD en relación con las transferencias internacionales de datos a países sin nivel de protección equivalente; (3) Información insuficiente en la cláusula informativa sobre el tratamiento de datos y los destinatarios de los mismos, incumpliendo el art. 13 RGPD. La AEPD valoró como agravante la condición de responsable del tratamiento como entidad bancaria de gran tamaño y con elevado volumen de clientes afectados.",
    topics: JSON.stringify(["transferencias", "consentimiento", "publicidad"]),
    gdpr_articles: JSON.stringify(["6", "7", "13", "44"]),
    status: "final",
  },
  // PS-00353-2019 — Equifax Ibérica (solicitantes de crédito)
  {
    reference: "PS-00353-2019",
    title: "Resolución AEPD — Equifax Ibérica (solvencia y ficheros de morosos)",
    date: "2019-11-28",
    type: "sancion",
    entity_name: "Equifax Ibérica, S.L.",
    fine_amount: 1_000_000,
    summary:
      "La AEPD impuso a Equifax Ibérica una sanción de 1 millón de euros por incluir en sus ficheros de solvencia datos de personas sin verificar la existencia real de la deuda, sin informar a los afectados conforme a las garantías establecidas por la LOPDGDD, y por ceder esos datos a terceros sin base legal.",
    full_text:
      "La Agencia Española de Protección de Datos ha impuesto a Equifax Ibérica, S.L. una sanción de 1.000.000 euros por infracciones graves del RGPD relacionadas con su actividad como gestor de ficheros de solvencia y morosidad. La AEPD investigó las prácticas de Equifax Ibérica en relación con la inclusión de datos de personas físicas en ficheros de información crediticia. Se constataron las siguientes infracciones: (1) Inclusión de datos en ficheros de solvencia sin verificar adecuadamente la existencia real de la deuda ni la legitimación del acreedor para la comunicación, incumpliendo los requisitos establecidos en el art. 20 de la LOPDGDD; (2) Información insuficiente a los afectados sobre su inclusión en los ficheros y sus derechos, incumpliendo el art. 14 RGPD; (3) Cesión de datos de solvencia a terceros (entidades financieras, empresas de telecomunicaciones, etc.) sin que se hubiera verificado que los datos incluidos cumplían con los requisitos legales exigibles; (4) Mantenimiento de datos en los ficheros de morosos más allá del plazo máximo de cinco años establecido en el art. 20.3 LOPDGDD. La AEPD valoró la especial sensibilidad de los datos de solvencia, dado que su inclusión incorrecta en ficheros de morosos puede tener graves consecuencias económicas para los afectados (denegación de créditos, contratos de telecomunicaciones, etc.).",
    topics: JSON.stringify(["derechos_interesados", "publicidad"]),
    gdpr_articles: JSON.stringify(["5", "6", "14"]),
    status: "final",
  },
];

const insertDecision = db.prepare(`
  INSERT OR IGNORE INTO decisions
    (reference, title, date, type, entity_name, fine_amount, summary, full_text, topics, gdpr_articles, status)
  VALUES
    (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
`);

const insertDecisionsAll = db.transaction(() => {
  for (const d of decisions) {
    insertDecision.run(
      d.reference,
      d.title,
      d.date,
      d.type,
      d.entity_name,
      d.fine_amount,
      d.summary,
      d.full_text,
      d.topics,
      d.gdpr_articles,
      d.status,
    );
  }
});

insertDecisionsAll();
console.log(`Inserted ${decisions.length} decisions`);

// --- Guidelines --------------------------------------------------------------

interface GuidelineRow {
  reference: string | null;
  title: string;
  date: string;
  type: string;
  summary: string;
  full_text: string;
  topics: string;
  language: string;
}

const guidelines: GuidelineRow[] = [
  {
    reference: "AEPD-GUIA-COOKIES-2023",
    title: "Guía sobre el uso de las cookies",
    date: "2023-07-01",
    type: "guia",
    summary:
      "Guía actualizada de la AEPD sobre el uso de cookies y tecnologías similares, incluyendo los requisitos de consentimiento, las excepciones para cookies técnicas, y las obligaciones de información. Incorpora las novedades del RGPD y la Directiva ePrivacy.",
    full_text:
      "La Agencia Española de Protección de Datos ha publicado esta guía actualizada sobre el uso de las cookies en el marco del RGPD y la Ley 34/2002, de Servicios de la Sociedad de la Información (LSSI). ¿Qué son las cookies? Las cookies son pequeños ficheros de información que se envían entre un servidor y un navegador web y que permiten al sitio web recordar información sobre la visita del usuario. ¿Cuándo se necesita consentimiento? Para el uso de cookies que no sean estrictamente necesarias para el funcionamiento del servicio solicitado por el usuario es necesario obtener su consentimiento previo, libre, específico, informado e inequívoco (art. 22.2 LSSI y art. 7 RGPD). Cookies exentas de consentimiento: las cookies estrictamente necesarias para la prestación de un servicio solicitado por el usuario están exentas del requisito de consentimiento. Ejemplos: cookies de sesión, cookies de autenticación de usuario, cookies de cesta de la compra, cookies de seguridad. Requisitos del consentimiento para cookies: (1) debe obtenerse antes de instalar las cookies; (2) debe ser específico para cada finalidad; (3) el rechazo debe ser tan sencillo como la aceptación; (4) los muros de cookies (cookie walls) que impiden el acceso al servicio sin aceptar cookies de terceros son en general contrarios al RGPD; (5) el consentimiento debe renovarse periódicamente (máximo 24 meses). Panel de preferencias de cookies: la AEPD recomienda implementar un panel de preferencias que permita al usuario aceptar o rechazar las cookies por categorías. La primera capa de información debe contener la información esencial de forma clara y accesible. Auditoría de cookies: los responsables del tratamiento deben realizar una auditoría periódica de las cookies instaladas en su sitio web para verificar su conformidad con el RGPD.",
    topics: JSON.stringify(["cookies", "consentimiento"]),
    language: "es",
  },
  {
    reference: "AEPD-GUIA-VIDEOVIGILANCIA-2022",
    title: "Guía sobre videovigilancia",
    date: "2022-06-01",
    type: "guia",
    summary:
      "Guía de la AEPD sobre los requisitos legales para el uso de sistemas de videovigilancia en establecimientos, lugares de trabajo y espacios públicos. Cubre las bases legales, la información a los interesados, los plazos de conservación, y el régimen especial de las cámaras de las Fuerzas y Cuerpos de Seguridad del Estado.",
    full_text:
      "La Agencia Española de Protección de Datos ha publicado esta guía sobre videovigilancia para informar a los responsables del tratamiento sobre sus obligaciones en relación con los sistemas de cámaras y circuitos de TV. Base legal para la videovigilancia: (1) Para las actividades privadas: la base legal más habitualmente invocada es el interés legítimo (art. 6.1.f RGPD), que debe superar el test de ponderación frente al derecho a la privacidad de los interesados. El art. 22 de la LOPDGDD establece los requisitos específicos para la videovigilancia con fines de seguridad; (2) Para actividades de seguridad pública: las Fuerzas y Cuerpos de Seguridad del Estado disponen de habilitaciones específicas en la LOPDGDD y la Ley Orgánica de Seguridad Ciudadana. Información a los interesados: las zonas videovigiladas deben estar señalizadas de forma visible mediante carteles que incluyan la identidad del responsable del tratamiento, la posibilidad de ejercer los derechos de acceso, rectificación, supresión y oposición, y la información sobre los plazos de conservación. La señalización debe ser visible al menos desde cinco metros de distancia. Conservación de imágenes: las imágenes captadas por videovigilancia no pueden conservarse por más de un mes (art. 22.3 LOPDGDD), salvo cuando las imágenes deban conservarse como prueba para procedimientos judiciales o administrativos. Cámaras en el lugar de trabajo: la videovigilancia de los trabajadores requiere una base legal específica y la información previa a los trabajadores y a sus representantes. Las cámaras ocultas en el lugar de trabajo están prohibidas salvo en casos excepcionales de sospecha fundada de actividad ilícita. Evaluación de impacto (EIPD): el uso de sistemas de videovigilancia masiva, con funcionalidades de reconocimiento facial, o en lugares donde acuden personas especialmente vulnerables, puede requerir la realización de una EIPD.",
    topics: JSON.stringify(["videovigilancia", "consentimiento"]),
    language: "es",
  },
  {
    reference: "AEPD-GUIA-INFORMAR-2021",
    title: "Guía para el cumplimiento del deber de informar",
    date: "2021-09-15",
    type: "guia",
    summary:
      "Guía de la AEPD sobre cómo cumplir con el deber de informar a los interesados sobre el tratamiento de sus datos personales conforme a los arts. 13 y 14 del RGPD, con modelos de cláusulas informativas y recomendaciones prácticas.",
    full_text:
      "La Agencia Española de Protección de Datos ha publicado esta guía para facilitar a los responsables del tratamiento el cumplimiento de las obligaciones de información establecidas en los artículos 13 y 14 del RGPD. Información a proporcionar cuando los datos se obtienen del interesado (art. 13 RGPD): (1) Identidad y datos de contacto del responsable del tratamiento; (2) Datos de contacto del delegado de protección de datos (DPD), en su caso; (3) Fines del tratamiento y base jurídica; (4) Intereses legítimos del responsable, cuando la base jurídica sea el interés legítimo; (5) Destinatarios o categorías de destinatarios; (6) Transferencias internacionales previstas, con indicación de las garantías adecuadas; (7) Plazo de conservación de los datos; (8) Derechos del interesado (acceso, rectificación, supresión, limitación, portabilidad, oposición); (9) Derecho a retirar el consentimiento en cualquier momento, cuando la base jurídica sea el consentimiento; (10) Derecho a presentar una reclamación ante la AEPD; (11) Si la comunicación de datos es un requisito legal o contractual, o necesaria para celebrar un contrato. Modelo de capas informativas: la AEPD recomienda un sistema de información por capas para mejorar la transparencia: una primera capa con la información esencial (identidad del responsable, finalidades, base jurídica y derechos), y una segunda capa con la información completa. Esta aproximación facilita que los interesados comprendan los aspectos más relevantes del tratamiento antes de acceder a la información completa. Lenguaje claro y sencillo: la información debe presentarse de forma concisa, transparente, inteligible y de fácil acceso, con un lenguaje claro y sencillo, especialmente cuando la información vaya dirigida a un menor.",
    topics: JSON.stringify(["derechos_interesados", "consentimiento", "menores"]),
    language: "es",
  },
  {
    reference: "AEPD-GUIA-EIPD-2021",
    title: "Guía práctica para las Evaluaciones de Impacto sobre la Protección de Datos (EIPD)",
    date: "2021-03-01",
    type: "guia",
    summary:
      "Guía práctica de la AEPD sobre la realización de Evaluaciones de Impacto sobre la Protección de Datos (EIPD) conforme al art. 35 RGPD. Incluye la lista de tratamientos que requieren EIPD obligatoria en España y metodología de evaluación.",
    full_text:
      "La Agencia Española de Protección de Datos ha publicado esta guía práctica sobre las Evaluaciones de Impacto sobre la Protección de Datos (EIPD), conocidas en inglés como Data Protection Impact Assessments (DPIA). ¿Cuándo es obligatoria una EIPD? El art. 35 RGPD establece que es obligatoria cuando el tratamiento sea probable que entrañe un alto riesgo para los derechos y libertades de las personas. La AEPD ha publicado una lista de tipos de tratamientos que requieren EIPD en España, incluyendo: (1) Tratamientos que impliquen elaboración de perfiles de personas físicas con efectos significativos; (2) Tratamientos a gran escala de categorías especiales de datos; (3) Tratamientos que impliquen observación sistemática de zonas de acceso público a gran escala; (4) Tratamientos de datos de personas vulnerables, especialmente menores; (5) Tratamientos que usen tecnologías nuevas o innovadoras. Fases de una EIPD: (1) Descripción del tratamiento: qué datos se tratan, con qué finalidad, quiénes son los responsables y destinatarios, dónde se almacenan los datos; (2) Evaluación de la necesidad y proporcionalidad: verificación de que el tratamiento es necesario para la finalidad, que existe una base legal adecuada, y que los derechos de los interesados pueden ejercerse; (3) Identificación y evaluación de riesgos: identificación de las amenazas (acceso ilegítimo, modificación no autorizada, destrucción de datos), evaluación de la probabilidad e impacto de cada amenaza; (4) Medidas previstas para afrontar los riesgos: medidas técnicas y organizativas para reducir los riesgos a un nivel aceptable. Consulta previa a la AEPD: cuando el análisis de riesgos revele que el tratamiento entrañaría un alto riesgo residual, el responsable deberá consultar previamente a la AEPD antes de proceder al tratamiento.",
    topics: JSON.stringify(["evaluacion_impacto", "videovigilancia", "transferencias"]),
    language: "es",
  },
];

const insertGuideline = db.prepare(`
  INSERT INTO guidelines (reference, title, date, type, summary, full_text, topics, language)
  VALUES (?, ?, ?, ?, ?, ?, ?, ?)
`);

const insertGuidelinesAll = db.transaction(() => {
  for (const g of guidelines) {
    insertGuideline.run(
      g.reference,
      g.title,
      g.date,
      g.type,
      g.summary,
      g.full_text,
      g.topics,
      g.language,
    );
  }
});

insertGuidelinesAll();
console.log(`Inserted ${guidelines.length} guidelines`);

// --- Summary -----------------------------------------------------------------

const decisionCount = (
  db.prepare("SELECT count(*) as cnt FROM decisions").get() as { cnt: number }
).cnt;
const guidelineCount = (
  db.prepare("SELECT count(*) as cnt FROM guidelines").get() as { cnt: number }
).cnt;
const topicCount = (
  db.prepare("SELECT count(*) as cnt FROM topics").get() as { cnt: number }
).cnt;
const decisionFtsCount = (
  db.prepare("SELECT count(*) as cnt FROM decisions_fts").get() as { cnt: number }
).cnt;
const guidelineFtsCount = (
  db.prepare("SELECT count(*) as cnt FROM guidelines_fts").get() as { cnt: number }
).cnt;

console.log(`\nDatabase summary:`);
console.log(`  Topics:         ${topicCount}`);
console.log(`  Decisions:      ${decisionCount} (FTS entries: ${decisionFtsCount})`);
console.log(`  Guidelines:     ${guidelineCount} (FTS entries: ${guidelineFtsCount})`);
console.log(`\nDone. Database ready at ${DB_PATH}`);

db.close();
