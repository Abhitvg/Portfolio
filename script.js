/**
 * Abhishek Singh — Senior SDE & Distributed Systems Engineer
 * Human-Crafted Client Logic, System Architecture Playground & Live Projects
 */

document.addEventListener('DOMContentLoaded', () => {
  initTheme();
  initNavigation();
  initArchitecturePlayground();
  initProjectFilters();
  initProjectModal();
  initCopyTriggers();
  initContactForm();
  initLiveClock();
});

/* ==========================================================================
   1. Theme Management (Minimalist Dark / Light Mode)
   ========================================================================== */
function initTheme() {
  const themeToggleBtn = document.getElementById('themeToggleBtn');
  const themeIcon = document.getElementById('themeIcon');
  const storedTheme = localStorage.getItem('theme');
  const currentTheme = storedTheme || 'dark';

  document.documentElement.setAttribute('data-theme', currentTheme);
  updateIcon(currentTheme);

  if (themeToggleBtn) {
    themeToggleBtn.addEventListener('click', () => {
      const active = document.documentElement.getAttribute('data-theme');
      const next = active === 'dark' ? 'light' : 'dark';
      document.documentElement.setAttribute('data-theme', next);
      localStorage.setItem('theme', next);
      updateIcon(next);
      showToast(`Switched to ${next === 'dark' ? 'Dark' : 'Light'} Mode`);
    });
  }

  function updateIcon(theme) {
    if (!themeIcon) return;
    themeIcon.className = theme === 'dark' ? 'fa-regular fa-moon' : 'fa-regular fa-sun';
  }
}

/* ==========================================================================
   2. Navigation & Mobile Drawer
   ========================================================================== */
function initNavigation() {
  const mobileToggle = document.getElementById('mobileToggle');
  const navMenu = document.getElementById('navMenu');
  const navLinks = document.querySelectorAll('.nav-link');
  const sections = document.querySelectorAll('section[id]');

  if (mobileToggle && navMenu) {
    mobileToggle.addEventListener('click', () => {
      navMenu.classList.toggle('open');
      const icon = mobileToggle.querySelector('i');
      if (icon) {
        icon.classList.toggle('fa-bars');
        icon.classList.toggle('fa-xmark');
      }
    });

    navLinks.forEach(link => {
      link.addEventListener('click', () => {
        navMenu.classList.remove('open');
        const icon = mobileToggle.querySelector('i');
        if (icon) {
          icon.classList.add('fa-bars');
          icon.classList.remove('fa-xmark');
        }
      });
    });
  }

  window.addEventListener('scroll', () => {
    let current = '';
    const scrollY = window.pageYOffset;

    sections.forEach(section => {
      const top = section.offsetTop - 120;
      const height = section.offsetHeight;
      if (scrollY >= top && scrollY < top + height) {
        current = section.getAttribute('id');
      }
    });

    navLinks.forEach(link => {
      link.classList.remove('active');
      if (link.getAttribute('href') === `#${current}`) {
        link.classList.add('active');
      }
    });
  });
}

/* ==========================================================================
   3. Interactive System Architecture & Code Playground
   ========================================================================== */
const architectureData = {
  'kafka': {
    title: 'Kafka Event Ingestion & Decoupled Pipelines',
    file: 'InventoryEventProducer.java',
    code: `<span class="ann">@Service</span>
<span class="kw">public class</span> <span class="fn">InventoryEventProducer</span> {
    <span class="kw">private final</span> KafkaTemplate&lt;String, InventoryUpdateEvent&gt; kafkaTemplate;
    <span class="kw">private static final</span> String TOPIC = <span class="str">"inventory.live.events"</span>;

    <span class="ann">@Transactional</span>
    <span class="kw">public void</span> <span class="fn">publishUpdate</span>(String sku, <span class="kw">int</span> delta, String warehouseId) {
        InventoryUpdateEvent event = <span class="kw">new</span> InventoryUpdateEvent(sku, delta, warehouseId, Instant.now());
        <span class="cm">// Partitioned by SKU to maintain strict per-item event ordering</span>
        kafkaTemplate.send(TOPIC, sku, event).whenComplete((result, ex) -> {
            <span class="kw">if</span> (ex != <span class="kw">null</span>) {
                log.error(<span class="str">"Failed to emit event for SKU: {}"</span>, sku, ex);
                <span class="cm">// Route to dead-letter queue (DLQ) for automated retry</span>
            }
        });
    }
}`,
    desc: 'At ATMA AI (atma-ai.co.in), replaced legacy hourly batch sync with a fault-tolerant Kafka event streaming backbone. Enables instantaneous multi-datacenter inventory updates with zero consumer lag and guaranteed ordering.',
    stats: [
      { val: '100%', desc: 'Real-time consistency' },
      { val: '< 25ms', desc: 'Event publish latency' }
    ]
  },
  'database': {
    title: 'Database Index & Execution Plan Optimization',
    file: 'MongoDB_Covered_Index.json & Oracle_View.sql',
    code: `<span class="cm">// 1. MongoDB Compound Covered Index (Slashing Read Latency)</span>
db.catalog_items.createIndex(
    { <span class="str">"organizationId"</span>: <span class="num">1</span>, <span class="str">"status"</span>: <span class="num">1</span>, <span class="str">"updatedAt"</span>: -<span class="num">1</span> },
    { <span class="str">"name"</span>: <span class="str">"idx_org_status_updated"</span>, background: <span class="kw">true</span> }
);

<span class="cm">// 2. Oracle SQL Materialized View for C-Level Dashboards</span>
<span class="kw">CREATE MATERIALIZED VIEW</span> mv_exec_daily_metrics
<span class="kw">BUILD IMMEDIATE REFRESH FAST ON COMMIT AS</span>
<span class="kw">SELECT</span> p.platform_id, TRUNC(t.created_at) <span class="kw">AS</span> metric_date,
       COUNT(t.id) <span class="kw">AS</span> total_transactions, SUM(t.amount) <span class="kw">AS</span> gross_volume
<span class="kw">FROM</span> transactions t <span class="kw">JOIN</span> platforms p <span class="kw">ON</span> t.platform_id = p.id
<span class="kw">GROUP BY</span> p.platform_id, TRUNC(t.created_at);`,
    desc: 'Analyzed database query execution plans (EXPLAIN ANALYZE) to eliminate expensive table scans. Converted high-frequency queries to covered indexes in MongoDB and implemented FAST REFRESH materialized views in Oracle SQL.',
    stats: [
      { val: '35%', desc: 'MongoDB read latency cut' },
      { val: '40%', desc: 'Oracle report speedup' }
    ]
  },
  'rag': {
    title: 'Local-First AI Vector Retrieval (Porcupine)',
    file: 'local-retrieval-engine.ts',
    code: `<span class="kw">import</span> { pipeline } <span class="kw">from</span> <span class="str">'@xenova/transformers'</span>;
<span class="kw">import</span> Database <span class="kw">from</span> <span class="str">'better-sqlite3-multiple-ciphers'</span>;

<span class="kw">export class</span> <span class="fn">LocalVectorEngine</span> {
  <span class="kw">private</span> embedder: <span class="kw">any</span>;
  <span class="kw">private</span> db: Database;

  <span class="kw">async</span> <span class="fn">init</span>() {
    <span class="cm">// Initialize quantized ONNX transformer model locally on desktop CPU</span>
    <span class="kw">this</span>.embedder = <span class="kw">await</span> pipeline(<span class="str">'feature-extraction'</span>, <span class="str">'Xenova/all-MiniLM-L6-v2'</span>);
    <span class="kw">this</span>.db = <span class="kw">new</span> Database(<span class="str">'secure_vault.enc'</span>);
    <span class="kw">this</span>.db.pragma(<span class="str">"key = 'aes256_enterprise_key'"</span>);
  }

  <span class="kw">async</span> <span class="fn">semanticSearch</span>(query: <span class="kw">string</span>, limit = <span class="num">5</span>) {
    <span class="kw">const</span> output = <span class="kw">await</span> <span class="kw">this</span>.embedder(query, { pooling: <span class="str">'mean'</span>, normalize: <span class="kw">true</span> });
    <span class="kw">return this</span>.queryCosineSimilarity(Array.from(output.data), limit);
  }
}`,
    desc: 'Engineered an offline-first semantic retrieval engine for desktop ERPs. Runs 100% locally on device CPU using ONNX Runtime with zero data transmission to external APIs, backed by AES-256 encrypted SQLite.',
    stats: [
      { val: '< 50ms', desc: 'Local vector search' },
      { val: '0 bytes', desc: 'External data leakage' }
    ]
  },
  'solr': {
    title: 'Apache Solr 50K-SKU Full-Text Search',
    file: 'solrconfig-schema.xml',
    code: `<span class="cm">&lt;!-- Custom Schema FieldType with Phonetic &amp; N-Gram Filtering --&gt;</span>
&lt;<span class="kw">fieldType</span> name=<span class="str">"text_autocomplete"</span> class=<span class="str">"solr.TextField"</span> positionIncrementGap=<span class="str">"100"</span>&gt;
  &lt;<span class="kw">analyzer</span> type=<span class="str">"index"</span>&gt;
    &lt;<span class="kw">tokenizer</span> class=<span class="str">"solr.StandardTokenizerFactory"</span>/&gt;
    &lt;<span class="kw">filter</span> class=<span class="str">"solr.LowerCaseFilterFactory"</span>/&gt;
    &lt;<span class="kw">filter</span> class=<span class="str">"solr.EdgeNGramFilterFactory"</span> minGramSize=<span class="str">"2"</span> maxGramSize=<span class="str">"15"</span>/&gt;
  &lt;/<span class="kw">analyzer</span>&gt;
  &lt;<span class="kw">analyzer</span> type=<span class="str">"query"</span>&gt;
    &lt;<span class="kw">tokenizer</span> class=<span class="str">"solr.StandardTokenizerFactory"</span>/&gt;
    &lt;<span class="kw">filter</span> class=<span class="str">"solr.LowerCaseFilterFactory"</span>/&gt;
  &lt;/<span class="kw">analyzer</span>&gt;
&lt;/<span class="kw">fieldType</span>&gt;`,
    desc: 'Configured faceted filtering, auto-complete, and spellcheck over a 50,000+ item SKU catalog. Tuned Lucene segment caches to achieve sub-180ms response times under heavy concurrent user searches.',
    stats: [
      { val: '+ 20%', desc: 'Search CTR conversion' },
      { val: '< 180ms', desc: 'Sub-second query time' }
    ]
  }
};

function initArchitecturePlayground() {
  const tabs = document.querySelectorAll('.playground-tab');
  const codeBox = document.getElementById('codeEditorBody');
  const fileName = document.getElementById('codeFileName');
  const blueprintTitle = document.getElementById('blueprintTitle');
  const blueprintDesc = document.getElementById('blueprintDesc');
  const statsContainer = document.getElementById('blueprintStats');

  if (!tabs.length || !codeBox) return;

  function setArchitecture(key) {
    const data = architectureData[key];
    if (!data) return;

    tabs.forEach(t => {
      t.classList.toggle('active', t.getAttribute('data-arch') === key);
    });

    fileName.textContent = data.file;
    codeBox.innerHTML = data.code;
    blueprintTitle.textContent = data.title;
    blueprintDesc.textContent = data.desc;

    statsContainer.innerHTML = '';
    data.stats.forEach(s => {
      const div = document.createElement('div');
      div.className = 'blueprint-stat';
      div.innerHTML = `<div class="val">${s.val}</div><div class="desc">${s.desc}</div>`;
      statsContainer.appendChild(div);
    });
  }

  tabs.forEach(tab => {
    tab.addEventListener('click', () => {
      const archKey = tab.getAttribute('data-arch');
      setArchitecture(archKey);
    });
  });

  setArchitecture('kafka');
}

/* ==========================================================================
   4. Project Filtering
   ========================================================================== */
function initProjectFilters() {
  const filterBtns = document.querySelectorAll('.filter-btn');
  const projectCards = document.querySelectorAll('.project-card');

  filterBtns.forEach(btn => {
    btn.addEventListener('click', () => {
      filterBtns.forEach(b => b.classList.remove('active'));
      btn.classList.add('active');

      const filter = btn.getAttribute('data-filter');

      projectCards.forEach(card => {
        const categories = card.getAttribute('data-category') || '';
        if (filter === 'all' || categories.includes(filter)) {
          card.style.display = 'flex';
        } else {
          card.style.display = 'none';
        }
      });
    });
  });
}

/* ==========================================================================
   5. Project Modal Details with Verified Live Production URLs
   ========================================================================== */
const projectDetails = {
  'atma': {
    title: 'ATMA AI – Distributed Microservices & Event Stream Engine',
    subtitle: 'Spring Boot 3, Kafka event streaming, Solr search & Mongo optimization',
    date: 'Jul 2025 – Present',
    category: 'Distributed Systems & Cloud',
    image: 'assets/project-distributed.jpg',
    liveUrl: 'https://www.atma-ai.co.in/',
    github: 'https://github.com/Abhitvg/ATMA-AI-Backend',
    tech: ['Java 8/17+', 'Spring Boot', 'Apache Kafka', 'Apache Solr', 'MongoDB', 'Oracle SQL', 'JUnit', 'Mockito'],
    overview: 'Production backend architecture powering ATMA AI and its enterprise platforms, serving 10,000+ daily active users with 99.9% uptime. Engineered for real-time Kafka event streaming, high-load Solr catalog search, and optimized execution plans.',
    highlights: [
      'Engineered Kafka-based event streaming pipelines that replaced legacy hourly cron batches, providing immediate consistency for live inventory and promotion updates.',
      'Re-indexed MongoDB collections and designed compound covered indexes, slashing average read latency by 35%.',
      'Deployed Apache Solr with custom phonetic tokenizers and faceted filters across 50,000+ catalog items, driving a 20% increase in search conversion.',
      'Rewrote Oracle SQL analytical execution plans and implemented materialized views for C-level dashboards, trimming generation time by 40%.',
      'Maintained 85%+ test coverage across all microservice contracts using JUnit and Mockito.'
    ]
  },
  'virasat': {
    title: 'CIET-NCERT Virasat – Cultural Heritage Explorer',
    subtitle: 'PAC Project 20.09: Survey of India SVG drill-down engine & 3D artifacts',
    date: 'Nov 2025 – Jun 2026',
    category: 'Frontend Architecture & WebGL',
    image: 'assets/project-virasat.webp',
    liveUrl: 'https://ncert-ruby.vercel.app/explore',
    github: 'https://github.com/Abhitvg/bharat-heritage',
    tech: ['React.js', '@react-three/fiber', 'Three.js', 'TopoJSON', 'SVG Engine', 'Local JSON Pipeline'],
    overview: 'A national cultural heritage platform developed for CIET-NCERT enabling students and researchers across India to explore geographic landmarks and historical artifacts with zero network latency.',
    highlights: [
      'Engineered a 100% local JSON data pipeline, eliminating external network round-trips and making the platform resilient in low-connectivity school labs.',
      'Architected a Survey of India-compliant SVG drill-down map engine featuring three-tier zoom logic (Country → State → District heritage nodes).',
      'Integrated @react-three/fiber and Three.js for interactive, photorealistic 3D artifact rendering directly in browser canvas with WebGL hardware acceleration.',
      'Optimized TopoJSON geometric boundaries and conducted automated load simulations, achieving 60fps rendering performance on low-spec hardware.'
    ]
  },
  'zubin-mehta': {
    title: 'Embassy of Israel – Zubin Mehta Cultural Portal',
    subtitle: 'Interactive frontend portal commemorating Maestro Zubin Mehta',
    date: 'May 2026',
    category: 'Frontend & Cultural Tech',
    image: 'assets/project-zubin.jpg',
    liveUrl: 'https://ipomehta-mural.com/',
    github: 'https://github.com/Abhitvg/zubin-mehta-project',
    tech: ['React.js', 'Modern CSS', 'Asset Pipelines', 'Lazy Loading', 'Responsive Audio/Visual'],
    overview: 'An interactive cultural initiative portal developed for the Embassy of Israel honoring the legendary conductor Zubin Mehta, showcasing orchestral archives, interactive timeline, and visual retrospectives.',
    highlights: [
      'Architected modular React component hierarchy with fine-grained state management to orchestrate high-resolution visual storytelling.',
      'Implemented progressive image loading, WebP asset compression, and lazy-loading wrappers to ensure instant First Contentful Paint (FCP).',
      'Designed responsive, accessible audio/visual galleries with keyboard navigation support compliant with WCAG 2.1 AA standards.'
    ]
  },
  'ierf': {
    title: 'Indian Eurasia Research Forum (IERF) Infrastructure',
    subtitle: 'Digital infrastructure for international geopolitics & research dissemination',
    date: 'Oct 2025 – Jul 2026',
    category: 'Digital Infrastructure',
    image: 'assets/project-ierf.png',
    liveUrl: 'https://www.indiaeurasia.org/',
    github: 'https://github.com/Abhitvg/india-eurasia-research-forum',
    tech: ['Next.js', 'TypeScript', 'Cloud Hosting', 'SEO Optimization', 'CI/CD Pipelines'],
    overview: 'Led technical initiatives as Technical Lead (Part-time), architecting digital publication and repository infrastructure for global research scholars, diplomatic think tanks, and policy analysis.',
    highlights: [
      'Orchestrated multi-region Next.js deployment with automated incremental static regeneration (ISR) for fast international page loads.',
      'Implemented automated document indexing and metadata pipelines for peer-reviewed working papers and monographs.',
      'Established GitHub Actions CI/CD workflows for automated accessibility audits, link verification, and security scanning.'
    ]
  },
  'doonsilk': {
    title: 'Uttarakhand Shashan Co-Operative / DoonSilk',
    subtitle: 'Real-time supply chain networking platform for artisan sericulture',
    date: 'Jan 2024',
    category: 'Full Stack & Supply Chain',
    image: 'assets/project-doonsilk.jpg',
    liveUrl: 'https://doonsilk.com/',
    github: 'https://github.com/Abhitvg/uk-coop-main',
    tech: ['MERN Stack', 'Next.js', 'MongoDB', 'Express', 'Node.js', 'REST APIs'],
    overview: 'A digital transformation platform built for state artisan cooperatives in Uttarakhand, streamlining the supply chain from raw cocoon harvesting to handloom distribution and direct consumer ordering.',
    highlights: [
      'Engineered normalized MongoDB NoSQL schemas for multi-tier product inventories, batch traceability, and artisan cooperative payouts.',
      'Developed RESTful endpoints with role-based JWT authorization for regional inspectors, warehouse managers, and retail distributors.',
      'Built reactive Next.js interfaces with real-time stock alert webhooks, cutting inventory reconciliation lag from days to seconds.'
    ]
  },
  'sheetal': {
    title: 'Sheetal Electrotech – Enterprise Industrial Portal',
    subtitle: 'Full-stack industrial web architecture with 3D product rendering & i18n',
    date: 'Oct 2026',
    category: 'Full Stack & 3D Web',
    image: 'assets/project-sheetal.webp',
    liveUrl: 'https://sheetal-electrotech-pvt-ltd.vercel.app/en',
    github: 'https://github.com/Abhitvg/Sheetal-Electrotech-Pvt.-Ltd',
    tech: ['Next.js', 'TypeScript', 'Three.js', 'Draco Compression', 'Tailwind CSS', 'Multi-Language i18n'],
    overview: 'Corporate web portal and interactive digital showroom built for Sheetal Electrotech Pvt Ltd, featuring interactive 3D lighting fixtures, comprehensive industrial specifications, multi-language internationalization, and dynamic catalog routing.',
    highlights: [
      'Integrated Three.js with Google Draco geometry compression to render interactive 3D electrical products smoothly in the browser with sub-second asset decode times.',
      'Engineered Next.js App Router localization supporting multi-lingual international markets with zero hydration layout shifts.',
      'Built automated catalog indexing and dynamic PDF specification sheet generation for enterprise commercial RFPs.'
    ]
  },
  'fashion-look': {
    title: 'Fashion Look – Bespoke Luxury Tailoring Platform',
    subtitle: 'Multilingual e-commerce & custom apparel tailoring portal',
    date: 'Sep 2026',
    category: 'Full Stack & E-Commerce',
    image: 'assets/project-fashion.png',
    liveUrl: 'https://www.fashion-look.in/en',
    github: 'https://github.com/Abhitvg/fashion-look',
    tech: ['Next.js', 'Firebase / Firestore', 'Tailwind CSS', 'TypeScript', 'Cloud Messaging'],
    overview: 'Full-stack luxury bespoke tailoring platform featuring custom fabric selection, measurement appointment booking, live order tracking, and localized customer experiences.',
    highlights: [
      'Architected reactive Firestore schemas for real-time customer measurement sync, appointment booking slots, and garment fabrication statuses.',
      'Designed responsive luxury-tier storefront with WebP asset optimization, achieving 95+ Mobile Lighthouse scores.',
      'Integrated Firebase Cloud Messaging for instant order phase transitions and WhatsApp consultation hooks.'
    ]
  },
  'porcupine': {
    title: 'Porcupine – Local-First AI Orchestrator',
    subtitle: 'Multi-agent ERP desktop application with secure offline RAG',
    date: 'February 2026',
    category: 'Local AI & Architecture',
    image: 'assets/project-porcupine.png',
    liveUrl: '',
    github: 'https://github.com/Abhitvg/porcupine',
    tech: ['Electron', 'Node.js', 'Next.js', 'SQLite (SQLCipher)', 'ONNX Runtime', 'Xenova Transformers', 'Tailwind CSS'],
    overview: 'Porcupine is an enterprise desktop orchestrator designed for offline-first AI workflows, strict privacy governance, and multi-agent ERP management, running 100% on device CPU.',
    highlights: [
      'Engineered an Electron-based multi-agent supervisor system with automatic LLM provider failover and an autonomous AI critique loop for output verification.',
      'Constructed a 100% local vector retrieval pipeline leveraging ONNX Runtime and Xenova browser/node transformers, delivering sub-50ms local semantic embeddings.',
      'Architected an encrypted SQLite storage engine with SQLCipher for confidential CRM, financial ledgers, and automated publishing workflows.',
      'Engineered a real-time reactive Next.js executive cockpit with IPC event streaming to monitor agent health, task queues, and memory usage.'
    ]
  },
  'media-bias': {
    title: 'Computational NLP Media Bias Detection Engine',
    subtitle: 'M.Tech Thesis at JNU: Social Network Analysis across 50,000+ articles',
    date: '2024 – 2025 (JNU Thesis)',
    category: 'Computational NLP & Graphs',
    image: 'assets/project-thesis.jpg',
    liveUrl: '',
    github: 'https://github.com/Abhitvg/Media-Bias-Analysis-in-Indian-News-Articles',
    tech: ['Python', 'NLP Pipelines', 'NetworkX', 'Graph Data Science', 'Topic Modeling', 'Pandas', 'BERT Embeddings'],
    overview: 'Master of Technology research thesis exploring computational media bias and agenda-setting in major Indian news publications. Processed a massive corpus of 50,000+ news articles with automated NLP entity extraction.',
    highlights: [
      'Built a distributed NLP scraping and ingestion pipeline that normalized over 50,000 news articles from top national digital dailies.',
      'Constructed social co-occurrence networks using NetworkX, revealing hidden framing topologies, sentiment skew, and entity centrality metrics.',
      'Implemented unsupervised topic modeling (LDA & BERTopic) combined with transformer sentiment scoring to quantify political and socioeconomic framing.',
      'Defended thesis successfully before the JNU School of Computer and Systems Sciences committee with distinction.'
    ]
  }
};

function initProjectModal() {
  const modal = document.getElementById('projectModal');
  const closeBtn = document.getElementById('modalCloseBtn');
  const projectCards = document.querySelectorAll('.project-card');

  if (!modal) return;

  projectCards.forEach(card => {
    card.addEventListener('click', (e) => {
      // If clicking directly on an external link inside the card, let default action proceed
      if (e.target.closest('a.external-card-link')) return;

      const id = card.getAttribute('data-project');
      const data = projectDetails[id];
      if (!data) return;

      document.getElementById('modalImg').src = data.image;
      document.getElementById('modalImg').alt = data.title;
      document.getElementById('modalCategory').textContent = data.category;
      document.getElementById('modalDate').textContent = data.date;
      document.getElementById('modalTitle').textContent = data.title;
      document.getElementById('modalSubtitle').textContent = data.subtitle;
      document.getElementById('modalOverview').textContent = data.overview;

      const highlights = document.getElementById('modalHighlights');
      highlights.innerHTML = '';
      data.highlights.forEach(h => {
        const li = document.createElement('li');
        li.textContent = h;
        highlights.appendChild(li);
      });

      const tech = document.getElementById('modalTech');
      tech.innerHTML = '';
      data.tech.forEach(t => {
        const span = document.createElement('span');
        span.className = 'tech-chip';
        span.textContent = t;
        tech.appendChild(span);
      });

      const ghLink = document.getElementById('modalGithubLink');
      if (ghLink) ghLink.href = data.github;

      const liveLink = document.getElementById('modalLiveLink');
      if (liveLink) {
        if (data.liveUrl) {
          liveLink.href = data.liveUrl;
          liveLink.style.display = 'inline-flex';
        } else {
          liveLink.style.display = 'none';
        }
      }

      modal.showModal();
      document.body.style.overflow = 'hidden';
    });
  });

  function closeModal() {
    modal.close();
    document.body.style.overflow = '';
  }

  if (closeBtn) closeBtn.addEventListener('click', closeModal);

  modal.addEventListener('click', (e) => {
    const rect = modal.getBoundingClientRect();
    if (
      e.clientX < rect.left || e.clientX > rect.right ||
      e.clientY < rect.top || e.clientY > rect.bottom
    ) {
      closeModal();
    }
  });

  modal.addEventListener('cancel', () => {
    document.body.style.overflow = '';
  });
}

/* ==========================================================================
   6. Copy Triggers & Toast Notifications
   ========================================================================== */
function initCopyTriggers() {
  const copyBtns = document.querySelectorAll('.copy-btn');

  copyBtns.forEach(btn => {
    btn.addEventListener('click', (e) => {
      e.stopPropagation();
      const text = btn.getAttribute('data-copy');
      if (!text) return;

      navigator.clipboard.writeText(text).then(() => {
        const prev = btn.innerHTML;
        btn.innerHTML = '<i class="fa-solid fa-check" style="color: var(--accent-emerald);"></i>';
        showToast(`Copied to clipboard: ${text}`);
        setTimeout(() => { btn.innerHTML = prev; }, 1800);
      });
    });
  });
}

function showToast(msg) {
  let container = document.querySelector('.toast-container');
  if (!container) {
    container = document.createElement('div');
    container.className = 'toast-container';
    document.body.appendChild(container);
  }

  const toast = document.createElement('div');
  toast.className = 'toast';
  toast.innerHTML = `<i class="fa-solid fa-check" style="color: var(--accent-emerald); font-size: 0.85rem;"></i> <span>${msg}</span>`;
  container.appendChild(toast);

  setTimeout(() => toast.classList.add('show'), 20);
  setTimeout(() => {
    toast.classList.remove('show');
    setTimeout(() => toast.remove(), 250);
  }, 2800);
}

/* ==========================================================================
   7. Contact Form
   ========================================================================== */
function initContactForm() {
  const form = document.getElementById('contactForm');
  if (!form) return;

  form.addEventListener('submit', (e) => {
    e.preventDefault();
    const name = document.getElementById('formName').value.trim();
    const email = document.getElementById('formEmail').value.trim();
    const subject = document.getElementById('formSubject').value.trim() || `Portfolio Inquiry from ${name}`;
    const message = document.getElementById('formMessage').value.trim();

    if (!name || !email || !message) return;

    const mailto = `mailto:Abhisheksingh1thakur@gmail.com?subject=${encodeURIComponent(subject)}&body=${encodeURIComponent(`From: ${name} (${email})\n\n${message}`)}`;
    window.location.href = mailto;
    showToast('Opening your email client...');
    form.reset();
  });
}

/* ==========================================================================
   8. Live IST Clock
   ========================================================================== */
function initLiveClock() {
  const clock = document.getElementById('liveClock');
  if (!clock) return;

  function tick() {
    const now = new Date();
    const str = new Intl.DateTimeFormat([], {
      timeZone: 'Asia/Kolkata',
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit',
      hour12: true
    }).format(now);
    clock.textContent = `${str} (IST, New Delhi)`;
  }

  tick();
  setInterval(tick, 1000);
}
