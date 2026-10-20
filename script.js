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
  initCommandPalette();
  initDocumentPreviewer();
  initCardSpotlight();
  initAnimatedCounters();
  initProjectDeepLinking();
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
  },
  'k8s': {
    title: 'Kubernetes Cluster Orchestration & Docker Containerization',
    file: 'k8s-deployment.yaml & Dockerfile',
    code: `<span class="cm"># 1. Multi-Stage Distroless Dockerfile (Spring Boot 3)</span>
<span class="kw">FROM</span> eclipse-temurin:17-jdk-alpine <span class="kw">AS</span> builder
<span class="kw">WORKDIR</span> /workspace
<span class="kw">COPY</span> . .
<span class="kw">RUN</span> ./mvnw clean package -DskipTests

<span class="kw">FROM</span> gcr.io/distroless/java17-debian11:nonroot
<span class="kw">COPY</span> --from=builder /workspace/target/*.jar app.jar
<span class="kw">USER</span> nonroot:nonroot
<span class="kw">ENTRYPOINT</span> [<span class="str">"java"</span>, <span class="str">"-XX:+UseG1GC"</span>, <span class="str">"-jar"</span>, <span class="str">"/app.jar"</span>]

<span class="cm"># 2. Kubernetes Deployment with Rolling Updates &amp; Health Probes</span>
<span class="kw">apiVersion</span>: apps/v1
<span class="kw">kind</span>: Deployment
<span class="kw">metadata</span>:
  <span class="kw">name</span>: atma-api-deployment
<span class="kw">spec</span>:
  <span class="kw">replicas</span>: 3
  <span class="kw">strategy</span>:
    <span class="kw">rollingUpdate</span>: { <span class="kw">maxSurge</span>: 1, <span class="kw">maxUnavailable</span>: 0 }
  <span class="kw">template</span>:
    <span class="kw">spec</span>:
      <span class="kw">containers</span>:
      - <span class="kw">name</span>: api-service
        <span class="kw">image</span>: registry.atma-ai.internal/api:v2.4
        <span class="kw">ports</span>: [{ <span class="kw">containerPort</span>: 8080 }]
        <span class="kw">livenessProbe</span>:
          <span class="kw">httpGet</span>: { <span class="kw">path</span>: /actuator/health/liveness, <span class="kw">port</span>: 8080 }
          <span class="kw">initialDelaySeconds</span>: 20
        <span class="kw">readinessProbe</span>:
          <span class="kw">httpGet</span>: { <span class="kw">path</span>: /actuator/health/readiness, <span class="kw">port</span>: 8080 }
        <span class="kw">resources</span>:
          <span class="kw">limits</span>: { <span class="kw">cpu</span>: <span class="str">"1000m"</span>, <span class="kw">memory</span>: <span class="str">"1Gi"</span> }
          <span class="kw">requests</span>: { <span class="kw">cpu</span>: <span class="str">"250m"</span>, <span class="kw">memory</span>: <span class="str">"512Mi"</span> }`,
    desc: 'Containerized Java Spring Boot microservices into lightweight non-root distroless Docker containers. Deployed across Kubernetes clusters with Horizontal Pod Autoscaling (HPA), zero-downtime rolling update strategies, and Actuator liveness/readiness probes.',
    stats: [
      { val: 'Zero', desc: 'Deployment downtime' },
      { val: '60%', desc: 'Docker image size cut' }
    ]
  },
  'diagram': {
    title: 'Enterprise Distributed System Topology',
    file: 'system-architecture-topology.svg',
    code: `<div class="arch-topology-view">
  <div class="arch-node-box" data-node="clients">
    <div class="node-icon"><i class="fa-solid fa-mobile-screen"></i></div>
    <div class="node-title">Web &amp; Mobile Clients</div>
    <div class="node-desc">10,000+ DAU • HTTPS / WSS</div>
  </div>
  <div class="arch-arrow"><i class="fa-solid fa-arrow-right"></i></div>
  <div class="arch-node-box" data-node="ingress">
    <div class="node-icon"><i class="fa-solid fa-dharmachakra"></i></div>
    <div class="node-title">Kubernetes Ingress</div>
    <div class="node-desc">TLS &amp; Rate Limiting</div>
  </div>
  <div class="arch-arrow"><i class="fa-solid fa-arrow-right"></i></div>
  <div class="arch-node-box active-node" data-node="spring">
    <div class="node-icon"><i class="fa-solid fa-leaf"></i></div>
    <div class="node-title">Spring Boot 3 Pods</div>
    <div class="node-desc">Temurin 17 • HPA Autoscale</div>
  </div>
  <div class="arch-arrow"><i class="fa-solid fa-arrow-right"></i></div>
  <div class="arch-node-box" data-node="kafka">
    <div class="node-icon"><i class="fa-solid fa-network-wired"></i></div>
    <div class="node-title">Kafka Event Bus</div>
    <div class="node-desc">&lt; 15ms Ingestion Lag</div>
  </div>
  <div class="arch-arrow"><i class="fa-solid fa-arrow-right"></i></div>
  <div class="arch-node-box" data-node="storage">
    <div class="node-icon"><i class="fa-solid fa-database"></i></div>
    <div class="node-title">Solr &amp; Mongo Cluster</div>
    <div class="node-desc">50K Catalog • Covered Index</div>
  </div>
</div>`,
    desc: 'High-level topology of production systems engineered for ATMA Research and enterprise clients. Traffic flows through TLS-terminated Kubernetes Ingress down to autoscaling Spring Boot microservice replicas, asynchronously decoupled through Apache Kafka partitions into optimized Apache Solr search nodes and MongoDB covered index clusters.',
    stats: [
      { val: '10K+', desc: 'Active daily users' },
      { val: '99.9%', desc: 'Verified SLA uptime' },
      { val: '< 15ms', desc: 'Event delivery lag' }
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
  const copyBtn = document.getElementById('codeCopyBtn');

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

  if (copyBtn) {
    copyBtn.addEventListener('click', () => {
      const textToCopy = codeBox.innerText || codeBox.textContent;
      navigator.clipboard.writeText(textToCopy).then(() => {
        const prevHtml = copyBtn.innerHTML;
        copyBtn.innerHTML = '<i class="fa-solid fa-check" style="color: var(--accent-emerald);"></i> <span>Copied!</span>';
        showToast('Snippet copied to clipboard');
        setTimeout(() => { copyBtn.innerHTML = prevHtml; }, 2000);
      });
    });
  }

  setArchitecture('kafka');
}

/* ==========================================================================
   4. Project Filtering, Tech Stack Chips & Real-Time Search
   ========================================================================== */
function initProjectFilters() {
  const filterBtns = document.querySelectorAll('.filter-btn');
  const techPills = document.querySelectorAll('.tech-pill-btn');
  const projectCards = document.querySelectorAll('.project-card');
  const searchInput = document.getElementById('projectSearchInput');
  const clearBtn = document.getElementById('searchClearBtn');
  const noProjectsMsg = document.getElementById('noProjectsMessage');
  const resetBtn = document.getElementById('resetProjectFiltersBtn');

  let activeCategory = 'all';
  let activeTech = 'all';
  let searchQuery = '';

  function applyFilters() {
    let visibleCount = 0;
    projectCards.forEach(card => {
      const categories = card.getAttribute('data-category') || '';
      const cardTech = (card.getAttribute('data-tech') || '').toLowerCase();
      const text = card.textContent.toLowerCase();
      
      const matchesCategory = (activeCategory === 'all' || categories.includes(activeCategory));
      const matchesTech = (activeTech === 'all' || cardTech.includes(activeTech.toLowerCase()) || text.includes(activeTech.toLowerCase()));
      const matchesSearch = (!searchQuery || text.includes(searchQuery) || cardTech.includes(searchQuery));

      if (matchesCategory && matchesTech && matchesSearch) {
        card.style.display = 'flex';
        visibleCount++;
      } else {
        card.style.display = 'none';
      }
    });

    if (noProjectsMsg) {
      noProjectsMsg.style.display = visibleCount === 0 ? 'block' : 'none';
    }
  }

  filterBtns.forEach(btn => {
    btn.addEventListener('click', () => {
      filterBtns.forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
      activeCategory = btn.getAttribute('data-filter') || 'all';
      applyFilters();
    });
  });

  techPills.forEach(pill => {
    pill.addEventListener('click', () => {
      techPills.forEach(p => p.classList.remove('active'));
      pill.classList.add('active');
      activeTech = pill.getAttribute('data-tech') || 'all';
      applyFilters();
    });
  });

  if (searchInput) {
    searchInput.addEventListener('input', (e) => {
      searchQuery = e.target.value.trim().toLowerCase();
      if (clearBtn) clearBtn.style.display = searchQuery ? 'block' : 'none';
      applyFilters();
    });
  }

  if (clearBtn) {
    clearBtn.addEventListener('click', () => {
      if (searchInput) searchInput.value = '';
      searchQuery = '';
      clearBtn.style.display = 'none';
      applyFilters();
      if (searchInput) searchInput.focus();
    });
  }

  if (resetBtn) {
    resetBtn.addEventListener('click', () => {
      if (searchInput) searchInput.value = '';
      searchQuery = '';
      if (clearBtn) clearBtn.style.display = 'none';
      activeCategory = 'all';
      activeTech = 'all';
      filterBtns.forEach(b => b.classList.toggle('active', b.getAttribute('data-filter') === 'all'));
      techPills.forEach(p => p.classList.toggle('active', p.getAttribute('data-tech') === 'all'));
      applyFilters();
    });
  }
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
    image: 'assets/project-atma.webp',
    gallery: [
      { src: 'assets/project-atma-1.webp', title: 'Enterprise AI Hub & Platform Architecture', desc: 'Distributed AI Services Serving 10,000+ Daily Active Users' },
      { src: 'assets/project-atma-2.webp', title: 'GenAI Solutions & Data Intelligence Modules', desc: 'Real-time Analytical Execution Plans & Microservice Contracts' },
      { src: 'assets/project-atma-3.webp', title: 'Kafka Event Streams & Solr Query Pipeline', desc: 'High-Throughput Cluster Resilience & Compound Index Optimization' }
    ],
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
    gallery: [
      { src: 'assets/project-virasat-1.webp', title: 'National Heritage Explorer Portal', desc: 'Bharat — Land of Timeless Heritage Educational Platform' },
      { src: 'assets/project-virasat-2.webp', title: 'Interactive 3D Spatial Heritage Map', desc: 'WebGL 3D Engine with Zero Network Round-Trip Latency' },
      { src: 'assets/project-virasat-3.webp', title: 'State Monument Cards & Curriculum Drill-down', desc: 'NCERT Survey of India-Compliant District Cultural Nodes' }
    ],
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
    image: 'assets/project-zubin.webp',
    gallery: [
      { src: 'assets/project-zubin-1.webp', title: 'Interactive Memorial Mural Canvas', desc: 'Commissioned by Embassy of Israel — High-Resolution Visual Retrospective' },
      { src: 'assets/project-zubin-2.webp', title: 'Concert Archives & Historic Philharmonic Letters', desc: 'Archival Recordings, Letters from Avital Barak & Diplomatic Timeline' }
    ],
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
    image: 'assets/project-ierf.webp',
    gallery: [
      { src: 'assets/project-ierf-1.webp', title: 'Geopolitical Research Hub & Policy Portal', desc: 'Strategic Eurasian Dialogue Platform for Global Scholars' },
      { src: 'assets/project-ierf-2.webp', title: 'Publications & Analytical Dispatches', desc: 'Automated Document Indexing & Incremental Static Regeneration' }
    ],
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
    image: 'assets/project-doonsilk.webp',
    gallery: [
      { src: 'assets/project-doonsilk-1.webp', title: 'Luxury Silk Brand Hero Showcase', desc: 'Embrace Elegance, Wear Doon Silk — Where Luxury Meets Tradition' },
      { src: 'assets/project-doonsilk-2.webp', title: 'Handwoven Pure Silk Saree Catalog', desc: 'Artisan Sericulture Catalog with Real-Time Stock Availability' },
      { src: 'assets/project-doonsilk-3.webp', title: 'Artisan Cooperative Weaving Standards', desc: 'Batch Traceability & Fair-Wage Payout Records for Rural Weavers' }
    ],
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
    title: 'Sheetal Electrotech – LED Lighting & OEM Manufacturing Portal',
    subtitle: 'Full-stack industrial web architecture with 3D product rendering & i18n',
    date: 'Oct 2026',
    category: 'LED Lighting & OEM',
    image: 'assets/project-sheetal.webp',
    gallery: [
      { src: 'assets/project-sheetal-1.webp', title: 'Industrial Manufacturing Showcase & Hero', desc: 'Solving Complex Manufacturing Challenges — LED Lighting, Electronics & OEM Solutions' },
      { src: 'assets/project-sheetal-2.webp', title: 'Commercial & Industrial Product Portfolio', desc: 'Comprehensive Grid of LED Bulbs, Downlights, Battens, Floodlights & Drivers' },
      { src: 'assets/project-sheetal-3.webp', title: 'High-Precision SMT & CNC Manufacturing Lines', desc: 'State-of-the-art SMT Line, CNC Tool Room & Daman Complex Plant Facilities' },
      { src: 'assets/project-sheetal-4.webp', title: 'Government BIS Testing & Technical Specs', desc: 'Certified Testing Labs, Photometric Compliance & Industrial Grade Standards' }
    ],
    liveUrl: 'https://sheetal-electrotech-pvt-ltd.vercel.app/en',
    github: 'https://github.com/Abhitvg/Sheetal-Electrotech-Pvt.-Ltd',
    tech: ['Next.js', 'TypeScript', 'Three.js', 'Draco Compression', 'Tailwind CSS', 'Multi-Language i18n'],
    overview: 'Corporate web portal and interactive digital showroom built for Sheetal Electrotech Pvt Ltd, featuring interactive 3D lighting fixtures, comprehensive industrial specifications, multi-language internationalization, and dynamic catalog routing.',
    highlights: [
      'Integrated Three.js with Google Draco geometry compression to render interactive 3D electrical products smoothly in the browser with sub-second asset decode times.',
      'Engineered Next.js App Router localization supporting multi-lingual international markets with zero hydration layout shifts.',
      'Built automated catalog indexing and dynamic PDF specification sheet generation for enterprise commercial RFPs.',
      'Showcased industrial manufacturing processes including surface mount technology (SMT) and CNC tooling facilities.'
    ]
  },
  'fashion-look': {
    title: 'Fashion Look – Bespoke Luxury Tailoring Platform',
    subtitle: 'Multilingual e-commerce & custom apparel tailoring portal',
    date: 'Sep 2026',
    category: 'Full Stack & E-Commerce',
    image: 'assets/project-fashion.webp',
    gallery: [
      { src: 'assets/project-fashion-1.webp', title: 'Bespoke Luxury Tailoring Storefront', desc: 'Handcrafted Sartorial Elegance & Custom Suit Consultation' },
      { src: 'assets/project-fashion-2.webp', title: 'Fabric Customizer & Product Catalog', desc: 'Interactive Fabric, Lapel, Button, and Cut Selection Engine' },
      { src: 'assets/project-fashion-3.webp', title: 'Inspiration Gallery & Client Lookbook', desc: 'Curated Collection Gallery with Real-Time Fitting Booking' }
    ],
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
    image: 'assets/project-porcupine.webp',
    gallery: [
      { src: 'assets/project-porcupine-1.webp', title: 'Local RAG Cockpit & Query Interface', desc: '100% On-Device Semantic Vector Search Running on CPU' },
      { src: 'assets/project-porcupine-2.webp', title: 'Vector Space & Embedding Topology', desc: 'ONNX Runtime High-Dimensional Retrieval & Agent Supervisor Loop' }
    ],
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
    image: 'assets/project-thesis.webp',
    gallery: [
      { src: 'assets/project-thesis-1.webp', title: 'NLP News Ingestion & Sentiment Topology', desc: 'JNU M.Tech Thesis: Deep Learning Analysis across 50,000+ National Articles' },
      { src: 'assets/project-thesis-2.webp', title: 'NetworkX Citation & Entity Centrality Graph', desc: 'Social Co-Occurrence Networks, Stance Topologies & Topic Modeling' }
    ],
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
  },
  'docflow': {
    title: 'DocFlow – Real-Time Collaborative Workspace',
    subtitle: 'Next.js 15, Tiptap 3 rich editor, Prisma ORM & live cloud collaboration',
    date: '2025 – 2026',
    category: 'Full Stack & Distributed State',
    image: 'assets/project-docflow.webp',
    gallery: [
      { src: 'assets/project-docflow-1.webp', title: 'Real-Time Document Editor Workspace', desc: 'Next.js 15 + Tiptap 3 Distraction-Free Rich Text Canvas' },
      { src: 'assets/project-docflow-2.webp', title: 'CRDT Multiplayer Cursors & Document Dashboard', desc: 'Sub-100ms Live Collaboration, Presence Awareness & Auto-Save Debouncing' }
    ],
    liveUrl: 'https://docflow-ashy.vercel.app',
    github: 'https://github.com/Abhitvg/docflow',
    tech: ['Next.js 15', 'React 19', 'Tiptap 3', 'Prisma 5', 'PostgreSQL', 'Tailwind CSS', 'Docker', 'Vercel'],
    overview: 'A modern, distraction-free collaborative document workspace engineered with Next.js 15 App Router, Tiptap rich-text extensions, and PostgreSQL via Prisma ORM. Designed for real-time document editing, slash commands, hierarchical document nesting, and sub-100ms sync.',
    highlights: [
      'Architected a modular document editing engine leveraging Tiptap 3 with custom extensions for markdown shortcuts, code blocks, task lists, and real-time cursor presence.',
      'Engineered schema-validated API routes with Prisma ORM and PostgreSQL connection pooling, handling nested page hierarchies and instant auto-save debouncing.',
      'Packaged containerized multi-stage Docker environment for automated development and localized testing.',
      'Deployed on Vercel with edge middleware authentication and instantaneous database indexing.'
    ]
  },
  'quorum': {
    title: 'Quorum – AI-Native Real-Time Video & Meeting Intelligence',
    subtitle: 'WebRTC, Live Speech-to-Text Transcription & Automated Action Item Synthesis',
    date: '2026',
    category: 'WebRTC & Audio AI',
    image: 'assets/project-quorum.webp',
    gallery: [
      { src: 'assets/project-quorum-1.webp', title: 'Video Conference Multi-Party Room Grid', desc: 'Low-Latency Peer-to-Peer & SFU Signaling Layer with Socket.io' },
      { src: 'assets/project-quorum-2.webp', title: 'Whisper AI Speech-to-Text & Diarization', desc: 'Asynchronous Audio Chunk Ingestion with Real-Time Subtitles & Minutes Export' }
    ],
    liveUrl: '',
    github: 'https://github.com/Abhitvg/Quorum',
    tech: ['WebRTC', 'Node.js', 'Socket.io', 'FastAPI', 'Whisper STT', 'Docker', 'Kubernetes', 'Redis'],
    overview: 'An AI-native collaborative meeting platform combining peer-to-peer WebRTC video streaming with real-time speech transcription, speaker diarization, and automated meeting minutes generation.',
    highlights: [
      'Architected low-latency WebRTC mesh and SFU signaling layer using Socket.io with adaptive bitrate streaming and auto-reconnection resilience.',
      'Engineered an asynchronous audio chunk ingestion pipeline feeding local Whisper models for sub-second live subtitle streaming.',
      'Implemented automated post-call summarization generating markdown agendas, key decisions, and Jira/Linear task export hooks.',
      'Containerized backend media and AI inference workers into Docker containers managed via Kubernetes manifests with autoscaling.'
    ]
  },
  'balancebite': {
    title: 'The Balance Bite – Nutrition & Meal Intelligence App',
    subtitle: 'Native Android Health & Diet Tracking with Firebase Realtime Sync',
    date: '2025',
    category: 'Mobile & Cloud',
    image: 'assets/project-balancebite.webp',
    gallery: [
      { src: 'assets/project-balancebite-1.webp', title: 'Native Android Material 3 Dashboard', desc: 'Realtime Calorie Rings, Macronutrient Progress & Daily Goals' },
      { src: 'assets/project-balancebite-2.webp', title: 'Macro Logging & Cloud Sync Engine', desc: 'Firebase Realtime DB Schemas with Offline Conflict Resolution' }
    ],
    liveUrl: '',
    github: 'https://github.com/Abhitvg/The-balance-Bite',
    tech: ['Android SDK', 'Java', 'Kotlin', 'Firebase Auth', 'Firebase Realtime DB', 'Material 3'],
    overview: 'An intuitive native Android mobile application for daily nutrition tracking, calorie management, and macronutrient balance with instantaneous Firebase cloud synchronization.',
    highlights: [
      'Designed responsive Material 3 Android interfaces featuring custom circular progress rings and animated nutrition breakdown bars.',
      'Engineered Firebase Realtime Database schemas with offline persistence and automatic conflict resolution for spotty network conditions.',
      'Implemented local caching and background synchronization services with WorkManager to maximize battery life.'
    ]
  }
};


function initProjectModal() {
  const modal = document.getElementById('projectModal');
  const closeBtn = document.getElementById('modalCloseBtn');
  const slidePrevBtn = document.getElementById('modalSlidePrevBtn');
  const slideNextBtn = document.getElementById('modalSlideNextBtn');
  const prevProjBtn = document.getElementById('modalPrevProjectBtn');
  const nextProjBtn = document.getElementById('modalNextProjectBtn');
  const indexCounter = document.getElementById('modalIndexCounter');
  const modalImg = document.getElementById('modalImg');
  const slideBadge = document.getElementById('modalSlideCounterBadge');
  const slideTitle = document.getElementById('modalSlideTitle');
  const slideDots = document.getElementById('modalSlideDots');
  const galleryStrip = document.getElementById('modalGalleryStrip');
  const projectCards = document.querySelectorAll('.project-card');

  if (!modal) return;

  const projectKeys = Object.keys(projectDetails);
  let currentModalIndex = 0;
  let currentSlideIndex = 0;

  function renderSlide(slideIdx) {
    const id = projectKeys[currentModalIndex];
    const data = projectDetails[id];
    if (!data || !data.gallery || data.gallery.length === 0) return;

    if (slideIdx < 0) slideIdx = data.gallery.length - 1;
    if (slideIdx >= data.gallery.length) slideIdx = 0;
    currentSlideIndex = slideIdx;

    const slide = data.gallery[slideIdx];
    modalImg.src = slide.src;
    modalImg.alt = slide.title;

    if (slideBadge) {
      slideBadge.innerHTML = `<i class="fa-solid fa-camera"></i> ${slideIdx + 1} / ${data.gallery.length}`;
    }
    if (slideTitle) {
      slideTitle.textContent = slide.title;
    }

    if (slideDots) {
      const dots = slideDots.querySelectorAll('.slide-dot');
      dots.forEach((dot, idx) => {
        dot.classList.toggle('active', idx === slideIdx);
      });
    }

    if (galleryStrip) {
      const thumbs = galleryStrip.querySelectorAll('.gallery-thumb-btn');
      thumbs.forEach((thumb, idx) => {
        thumb.classList.toggle('active', idx === slideIdx);
        if (idx === slideIdx) {
          // Safely scroll ONLY the horizontal galleryStrip without scrolling parent modal
          const thumbLeft = thumb.offsetLeft;
          const thumbWidth = thumb.offsetWidth;
          const stripWidth = galleryStrip.clientWidth;
          galleryStrip.scrollTo({
            left: thumbLeft - (stripWidth / 2) + (thumbWidth / 2),
            behavior: 'smooth'
          });
        }
      });
    }
  }

  function renderModalProject(index) {
    if (index < 0) index = projectKeys.length - 1;
    if (index >= projectKeys.length) index = 0;
    currentModalIndex = index;
    currentSlideIndex = 0;

    const modalWrap = modal.querySelector('.modal-wrap');
    if (modalWrap) {
      modalWrap.scrollTop = 0;
    }

    const id = projectKeys[index];
    const data = projectDetails[id];
    if (!data) return;

    document.getElementById('modalCategory').textContent = data.category;
    document.getElementById('modalDate').textContent = data.date;
    document.getElementById('modalTitle').textContent = data.title;
    document.getElementById('modalSubtitle').textContent = data.subtitle;
    document.getElementById('modalOverview').textContent = data.overview;

    if (indexCounter) {
      indexCounter.textContent = `Project ${index + 1} of ${projectKeys.length}`;
    }

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

    // Build Gallery Dots & Strip
    const gallery = data.gallery || [{ src: data.image, title: data.title, desc: data.subtitle }];
    
    if (slideDots) {
      slideDots.innerHTML = '';
      gallery.forEach((g, gIdx) => {
        const dot = document.createElement('button');
        dot.className = `slide-dot ${gIdx === 0 ? 'active' : ''}`;
        dot.setAttribute('aria-label', `Go to photo ${gIdx + 1}`);
        dot.addEventListener('click', (e) => {
          e.stopPropagation();
          renderSlide(gIdx);
        });
        slideDots.appendChild(dot);
      });
      slideDots.style.display = gallery.length > 1 ? 'flex' : 'none';
    }

    if (galleryStrip) {
      galleryStrip.innerHTML = '';
      gallery.forEach((g, gIdx) => {
        const thumb = document.createElement('button');
        thumb.className = `gallery-thumb-btn ${gIdx === 0 ? 'active' : ''}`;
        thumb.setAttribute('aria-label', g.title);
        thumb.title = g.title;
        thumb.innerHTML = `<img src="${g.src}" alt="${g.title}" loading="lazy">`;
        thumb.addEventListener('click', (e) => {
          e.stopPropagation();
          renderSlide(gIdx);
        });
        galleryStrip.appendChild(thumb);
      });
      galleryStrip.style.display = gallery.length > 1 ? 'flex' : 'none';
    }

    if (slidePrevBtn && slideNextBtn) {
      const showNav = gallery.length > 1;
      slidePrevBtn.style.display = showNav ? 'flex' : 'none';
      slideNextBtn.style.display = showNav ? 'flex' : 'none';
    }

    renderSlide(0);
  }

  window.renderModalProject = renderModalProject;
  window.openProjectModalById = function(id) {
    const idx = projectKeys.indexOf(id);
    if (idx !== -1) {
      renderModalProject(idx);
      modal.showModal();
      const wrap = modal.querySelector('.modal-wrap');
      if (wrap) wrap.scrollTop = 0;
      document.body.style.overflow = 'hidden';
    }
  };

  projectCards.forEach(card => {
    card.addEventListener('click', (e) => {
      if (e.target.closest('a.external-card-link')) return;
      const id = card.getAttribute('data-project');
      const idx = projectKeys.indexOf(id);
      if (idx !== -1) {
        renderModalProject(idx);
        modal.showModal();
        const wrap = modal.querySelector('.modal-wrap');
        if (wrap) wrap.scrollTop = 0;
        document.body.style.overflow = 'hidden';
      }
    });
  });

  // Photo Carousel Next / Prev
  if (slidePrevBtn) {
    slidePrevBtn.addEventListener('click', (e) => {
      e.stopPropagation();
      renderSlide(currentSlideIndex - 1);
    });
  }

  if (slideNextBtn) {
    slideNextBtn.addEventListener('click', (e) => {
      e.stopPropagation();
      renderSlide(currentSlideIndex + 1);
    });
  }

  // Project Switcher Next / Prev
  if (prevProjBtn) {
    prevProjBtn.addEventListener('click', (e) => {
      e.stopPropagation();
      renderModalProject(currentModalIndex - 1);
    });
  }

  if (nextProjBtn) {
    nextProjBtn.addEventListener('click', (e) => {
      e.stopPropagation();
      renderModalProject(currentModalIndex + 1);
    });
  }

  // Touch Swipe on Banner
  const banner = document.querySelector('.modal-banner');
  if (banner) {
    let touchStartX = 0;
    let touchEndX = 0;
    banner.addEventListener('touchstart', (e) => {
      touchStartX = e.changedTouches[0].screenX;
    }, { passive: true });
    banner.addEventListener('touchend', (e) => {
      touchEndX = e.changedTouches[0].screenX;
      if (touchStartX - touchEndX > 45) {
        renderSlide(currentSlideIndex + 1);
      } else if (touchEndX - touchStartX > 45) {
        renderSlide(currentSlideIndex - 1);
      }
    }, { passive: true });
  }

  const shareBtn = document.getElementById('modalShareBtn');
  if (shareBtn) {
    shareBtn.addEventListener('click', (e) => {
      e.stopPropagation();
      const currentId = projectKeys[currentModalIndex];
      const url = `${window.location.origin}${window.location.pathname}#project=${currentId}`;
      if (navigator.clipboard && navigator.clipboard.writeText) {
        navigator.clipboard.writeText(url).then(() => {
          showToast('Direct link to project copied to clipboard!');
        }).catch(() => {
          prompt('Copy project link:', url);
        });
      } else {
        prompt('Copy project link:', url);
      }
    });
  }

  function closeModal() {
    modal.close();
    document.body.style.overflow = '';
    if (window.location.hash && window.location.hash.startsWith('#project=')) {
      history.replaceState(null, null, window.location.pathname);
    }
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
    closeModal();
  });

  window.addEventListener('keydown', (e) => {
    if (!modal.open) return;
    if (e.key === 'ArrowLeft') {
      e.preventDefault();
      if (e.shiftKey) {
        renderModalProject(currentModalIndex - 1);
      } else {
        renderSlide(currentSlideIndex - 1);
      }
    } else if (e.key === 'ArrowRight') {
      e.preventDefault();
      if (e.shiftKey) {
        renderModalProject(currentModalIndex + 1);
      } else {
        renderSlide(currentSlideIndex + 1);
      }
    } else if (e.key.toLowerCase() === 'p') {
      renderModalProject(currentModalIndex - 1);
    } else if (e.key.toLowerCase() === 'n') {
      renderModalProject(currentModalIndex + 1);
    }
  });
}

/* ==========================================================================
   5b. Animated Metric Counters
   ========================================================================== */
function initAnimatedCounters() {
  const strip = document.getElementById('metricsStrip');
  const counters = document.querySelectorAll('.counter-num');
  if (!strip || !counters.length) return;

  let hasAnimated = false;

  const observer = new IntersectionObserver((entries) => {
    entries.forEach(entry => {
      if (entry.isIntersecting && !hasAnimated) {
        hasAnimated = true;
        counters.forEach(counter => {
          const target = parseFloat(counter.getAttribute('data-target')) || 0;
          const decimals = parseInt(counter.getAttribute('data-decimals') || '0', 10);
          const prefix = counter.getAttribute('data-prefix') || '';
          const suffix = counter.getAttribute('data-suffix') || '';
          const duration = 1500;
          const startTime = performance.now();

          function updateCount(currentTime) {
            const elapsed = currentTime - startTime;
            const progress = Math.min(elapsed / duration, 1);
            const ease = progress === 1 ? 1 : 1 - Math.pow(2, -10 * progress);
            const currentVal = (target * ease).toFixed(decimals);
            counter.textContent = `${prefix}${currentVal}${suffix}`;

            if (progress < 1) {
              requestAnimationFrame(updateCount);
            } else {
              counter.textContent = `${prefix}${target.toFixed(decimals)}${suffix}`;
            }
          }

          requestAnimationFrame(updateCount);
        });
        observer.disconnect();
      }
    });
  }, { threshold: 0.25 });

  observer.observe(strip);
}

/* ==========================================================================
   5c. Deep Linking URL Routing
   ========================================================================== */
function initProjectDeepLinking() {
  if (window.location.hash && window.location.hash.startsWith('#project=')) {
    const projId = window.location.hash.replace('#project=', '').trim();
    if (window.openProjectModalById) {
      setTimeout(() => {
        window.openProjectModalById(projId);
      }, 350);
    }
  }
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

/* ==========================================================================
   9. Command Palette (Cmd + K)
   ========================================================================== */
function initCommandPalette() {
  const dialog = document.getElementById('cmdPaletteDialog');
  const triggerBtn = document.getElementById('cmdKTrigger');
  const input = document.getElementById('cmdPaletteInput');
  const resultsContainer = document.getElementById('cmdPaletteResults');

  if (!dialog || !input || !resultsContainer) return;

  function scrollToSection(selector) {
    const el = document.querySelector(selector);
    if (el) {
      el.scrollIntoView({ behavior: 'smooth' });
    }
  }

  const commands = [
    // Navigation
    { type: 'nav', title: 'About Abhishek', icon: 'fa-regular fa-user', action: () => scrollToSection('#about'), badge: 'Section' },
    { type: 'nav', title: 'Experience & History', icon: 'fa-solid fa-briefcase', action: () => scrollToSection('#experience'), badge: 'Section' },
    { type: 'nav', title: 'Architecture Playground', icon: 'fa-solid fa-terminal', action: () => scrollToSection('#architecture'), badge: 'Section' },
    { type: 'nav', title: 'Live Projects (12 Total)', icon: 'fa-solid fa-layer-group', action: () => scrollToSection('#projects'), badge: 'Section' },
    { type: 'nav', title: 'Technical Skills Matrix', icon: 'fa-solid fa-gears', action: () => scrollToSection('#skills'), badge: 'Section' },
    { type: 'nav', title: 'Education & GATE / JEE', icon: 'fa-solid fa-graduation-cap', action: () => scrollToSection('#education'), badge: 'Section' },
    { type: 'nav', title: 'Verified Credentials & Dossiers', icon: 'fa-solid fa-certificate', action: () => scrollToSection('#credentials'), badge: 'Section' },
    { type: 'nav', title: 'Get In Touch', icon: 'fa-regular fa-envelope', action: () => scrollToSection('#contact'), badge: 'Section' },

    // Actions
    { type: 'action', title: 'Download Resume (PDF)', icon: 'fa-solid fa-file-pdf', action: () => { window.open('assets/Abhishek_Singh_Resume.pdf', '_blank'); }, badge: 'Download' },
    { type: 'action', title: 'Copy Email Address', icon: 'fa-regular fa-copy', action: () => { navigator.clipboard.writeText('Abhisheksingh1thakur@gmail.com'); showToast('Copied email to clipboard!'); }, badge: 'Action' },
    { type: 'action', title: 'View Embassy of Israel Letter', icon: 'fa-solid fa-award', action: () => window.openDocPreview('assets/Letter_of_Recommendation_Embassy_of_Israel.pdf', 'Embassy of Israel Recommendation', 'Signed by Avital Barak, Cultural Attaché'), badge: 'PDF' },
    { type: 'action', title: 'View NCERT Project 20.09 Dossier', icon: 'fa-solid fa-file-contract', action: () => window.openDocPreview('assets/Bharat_Heritage_Project_Dossier.pdf', 'NCERT Bharat Heritage Dossier', 'Technical Compliance Build'), badge: 'PDF' },
    { type: 'action', title: 'View JNU M.Tech Dissertation', icon: 'fa-solid fa-book-bookmark', action: () => window.openDocPreview('assets/Abhishek_Singh_MTech_Thesis_JNU.pdf', 'JNU M.Tech Dissertation', 'Computational NLP & Social Networks'), badge: 'PDF' },
    { type: 'action', title: 'Open GitHub Profile (@Abhitvg)', icon: 'fa-brands fa-github', action: () => { window.open('https://github.com/Abhitvg', '_blank'); }, badge: 'External' },
    { type: 'action', title: 'Toggle Dark / Light Theme', icon: 'fa-solid fa-circle-half-stroke', action: () => { document.getElementById('themeToggleBtn')?.click(); }, badge: 'Theme' },

    // Projects
    { type: 'project', title: 'ATMA AI – Distributed Microservices Hub', icon: 'fa-solid fa-cube', action: () => window.openProjectModalById('atma'), badge: 'Kafka / Java' },
    { type: 'project', title: 'NCERT Virasat – Cultural Heritage Explorer', icon: 'fa-solid fa-cube', action: () => window.openProjectModalById('virasat'), badge: 'React / Three.js' },
    { type: 'project', title: 'DocFlow – Real-Time Collaborative Workspace', icon: 'fa-solid fa-cube', action: () => window.openProjectModalById('docflow'), badge: 'Next.js 15 / Tiptap' },
    { type: 'project', title: 'Quorum – AI Video & Meeting Intelligence', icon: 'fa-solid fa-cube', action: () => window.openProjectModalById('quorum'), badge: 'WebRTC / Whisper' },
    { type: 'project', title: 'The Balance Bite – Nutrition Tracker', icon: 'fa-solid fa-cube', action: () => window.openProjectModalById('balancebite'), badge: 'Android / Firebase' },
    { type: 'project', title: 'Zubin Mehta Cultural Portal', icon: 'fa-solid fa-cube', action: () => window.openProjectModalById('zubin-mehta'), badge: 'Embassy of Israel' },
    { type: 'project', title: 'Indian Eurasia Research Forum (IERF)', icon: 'fa-solid fa-cube', action: () => window.openProjectModalById('ierf'), badge: 'Next.js / ISR' },
    { type: 'project', title: 'DoonSilk – Co-Operative Supply Chain', icon: 'fa-solid fa-cube', action: () => window.openProjectModalById('doonsilk'), badge: 'MERN Stack' },
    { type: 'project', title: 'Sheetal Electrotech – 3D Industrial Portal', icon: 'fa-solid fa-cube', action: () => window.openProjectModalById('sheetal'), badge: 'Three.js / Draco' },
    { type: 'project', title: 'Fashion Look – Luxury Tailoring Storefront', icon: 'fa-solid fa-cube', action: () => window.openProjectModalById('fashion-look'), badge: 'Next.js / Firebase' },
    { type: 'project', title: 'Porcupine – Local-First AI Orchestrator', icon: 'fa-solid fa-cube', action: () => window.openProjectModalById('porcupine'), badge: 'Local RAG' },
    { type: 'project', title: 'NLP Media Bias Detection Engine', icon: 'fa-solid fa-cube', action: () => window.openProjectModalById('media-bias'), badge: 'JNU M.Tech Thesis' }
  ];

  let selectedIndex = 0;
  let filteredCommands = [...commands];

  function renderList() {
    resultsContainer.innerHTML = '';
    if (filteredCommands.length === 0) {
      resultsContainer.innerHTML = `<div style="padding: 1.5rem; text-align: center; color: var(--text-muted); font-size: 0.88rem;">No commands or projects found matching search.</div>`;
      return;
    }

    let currentGroup = '';
    filteredCommands.forEach((cmd, idx) => {
      let groupName = cmd.type === 'nav' ? 'Navigation' : (cmd.type === 'action' ? 'Quick Actions' : 'Projects');
      if (groupName !== currentGroup) {
        currentGroup = groupName;
        const groupEl = document.createElement('div');
        groupEl.className = 'cmd-group-title';
        groupEl.textContent = groupName;
        resultsContainer.appendChild(groupEl);
      }

      const item = document.createElement('div');
      item.className = 'cmd-item' + (idx === selectedIndex ? ' active' : '');
      item.innerHTML = `
        <div class="cmd-item-left">
          <i class="${cmd.icon} cmd-item-icon"></i>
          <span>${cmd.title}</span>
        </div>
        <span class="cmd-item-badge">${cmd.badge}</span>
      `;

      item.addEventListener('click', () => {
        closePalette();
        cmd.action();
      });

      resultsContainer.appendChild(item);
    });

    const activeEl = resultsContainer.querySelector('.cmd-item.active');
    if (activeEl) activeEl.scrollIntoView({ block: 'nearest' });
  }

  function openPalette() {
    dialog.showModal();
    document.body.style.overflow = 'hidden';
    input.value = '';
    filteredCommands = [...commands];
    selectedIndex = 0;
    renderList();
    setTimeout(() => input.focus(), 60);
  }

  function closePalette() {
    dialog.close();
    document.body.style.overflow = '';
  }

  if (triggerBtn) triggerBtn.addEventListener('click', openPalette);

  window.addEventListener('keydown', (e) => {
    if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === 'k') {
      e.preventDefault();
      if (dialog.open) {
        closePalette();
      } else {
        openPalette();
      }
    } else if (dialog.open) {
      if (e.key === 'ArrowDown') {
        e.preventDefault();
        selectedIndex = (selectedIndex + 1) % filteredCommands.length;
        renderList();
      } else if (e.key === 'ArrowUp') {
        e.preventDefault();
        selectedIndex = (selectedIndex - 1 + filteredCommands.length) % filteredCommands.length;
        renderList();
      } else if (e.key === 'Enter') {
        e.preventDefault();
        if (filteredCommands[selectedIndex]) {
          const action = filteredCommands[selectedIndex].action;
          closePalette();
          action();
        }
      } else if (e.key === 'Escape') {
        closePalette();
      }
    }
  });

  input.addEventListener('input', (e) => {
    const q = e.target.value.toLowerCase().trim();
    if (!q) {
      filteredCommands = [...commands];
    } else {
      filteredCommands = commands.filter(c =>
        c.title.toLowerCase().includes(q) ||
        c.badge.toLowerCase().includes(q)
      );
    }
    selectedIndex = 0;
    renderList();
  });

  dialog.addEventListener('click', (e) => {
    const rect = dialog.getBoundingClientRect();
    if (e.clientX < rect.left || e.clientX > rect.right || e.clientY < rect.top || e.clientY > rect.bottom) {
      closePalette();
    }
  });
}

/* ==========================================================================
   10. Document Previewer Modal
   ========================================================================== */
function initDocumentPreviewer() {
  const dialog = document.getElementById('docPreviewDialog');
  const closeBtn = document.getElementById('docPreviewCloseBtn');
  const titleEl = document.getElementById('docPreviewTitle');
  const metaEl = document.getElementById('docPreviewMeta');
  const iframe = document.getElementById('docPreviewIframe');
  const downloadBtn = document.getElementById('docPreviewDownloadBtn');
  const previewBtns = document.querySelectorAll('.preview-doc-btn');

  if (!dialog || !iframe) return;

  window.openDocPreview = function(src, title, meta) {
    if (titleEl) titleEl.textContent = title;
    if (metaEl) metaEl.textContent = meta;
    iframe.src = src;
    if (downloadBtn) {
      downloadBtn.href = src;
      downloadBtn.setAttribute('download', src.split('/').pop());
    }
    dialog.showModal();
    document.body.style.overflow = 'hidden';
  };

  previewBtns.forEach(btn => {
    btn.addEventListener('click', (e) => {
      e.preventDefault();
      const src = btn.getAttribute('data-doc-src');
      const title = btn.getAttribute('data-doc-title') || 'Document';
      const meta = btn.getAttribute('data-doc-meta') || 'Verified Document';
      window.openDocPreview(src, title, meta);
    });
  });

  function closeDocPreview() {
    dialog.close();
    iframe.src = '';
    document.body.style.overflow = '';
  }

  if (closeBtn) closeBtn.addEventListener('click', closeDocPreview);

  dialog.addEventListener('click', (e) => {
    const rect = dialog.getBoundingClientRect();
    if (e.clientX < rect.left || e.clientX > rect.right || e.clientY < rect.top || e.clientY > rect.bottom) {
      closeDocPreview();
    }
  });

  dialog.addEventListener('cancel', () => {
    iframe.src = '';
    document.body.style.overflow = '';
  });
}

/* ==========================================================================
   11. Ambient Card Spotlight Illumination
   ========================================================================== */
function initCardSpotlight() {
  const interactiveCards = document.querySelectorAll('.project-card, .experience-card, .playground-card, .doc-card, .skills-group');

  interactiveCards.forEach(card => {
    card.addEventListener('mousemove', (e) => {
      const rect = card.getBoundingClientRect();
      const x = e.clientX - rect.left;
      const y = e.clientY - rect.top;
      card.style.setProperty('--mouse-x', `${x}px`);
      card.style.setProperty('--mouse-y', `${y}px`);
    });
  });
}

