/**
 * Abhishek Singh — Senior SDE & Distributed Systems Portfolio
 * Interactive Functionality & Modern UI Engines
 */

document.addEventListener('DOMContentLoaded', () => {
  initTheme();
  initCanvasBackground();
  initNavigation();
  initTerminal();
  initProjectFilters();
  initProjectModal();
  initCopyButtons();
  initContactForm();
  initLiveClock();
});

/* ==========================================================================
   1. Theme Management (Dark / Light with LocalStorage)
   ========================================================================== */
function initTheme() {
  const themeToggleBtn = document.getElementById('themeToggleBtn');
  const themeIcon = document.getElementById('themeIcon');
  const storedTheme = localStorage.getItem('theme');
  const prefersDark = window.matchMedia('(prefers-color-scheme: dark)').matches;
  
  const currentTheme = storedTheme || (prefersDark ? 'dark' : 'dark');
  document.documentElement.setAttribute('data-theme', currentTheme);
  updateThemeIcon(currentTheme);

  if (themeToggleBtn) {
    themeToggleBtn.addEventListener('click', () => {
      const activeTheme = document.documentElement.getAttribute('data-theme');
      const newTheme = activeTheme === 'dark' ? 'light' : 'dark';
      document.documentElement.setAttribute('data-theme', newTheme);
      localStorage.setItem('theme', newTheme);
      updateThemeIcon(newTheme);
      showToast(`Switched to ${newTheme === 'dark' ? 'Dark' : 'Light'} Mode`, 'info');
    });
  }

  function updateThemeIcon(theme) {
    if (!themeIcon) return;
    if (theme === 'dark') {
      themeIcon.className = 'fa-solid fa-moon';
    } else {
      themeIcon.className = 'fa-solid fa-sun';
    }
  }
}

/* ==========================================================================
   2. High Performance Interactive Canvas Particle Network
   ========================================================================== */
function initCanvasBackground() {
  const canvas = document.getElementById('bg-canvas');
  if (!canvas) return;
  const ctx = canvas.getContext('2d');
  let animationFrameId;
  let width, height;
  let particles = [];
  
  const mouse = { x: null, y: null, radius: 150 };

  window.addEventListener('mousemove', (e) => {
    mouse.x = e.x;
    mouse.y = e.y;
  });

  window.addEventListener('mouseout', () => {
    mouse.x = null;
    mouse.y = null;
  });

  function resize() {
    width = canvas.width = window.innerWidth;
    height = canvas.height = window.innerHeight;
    createParticles();
  }

  window.addEventListener('resize', resize);

  class Particle {
    constructor() {
      this.x = Math.random() * width;
      this.y = Math.random() * height;
      this.vx = (Math.random() - 0.5) * 0.6;
      this.vy = (Math.random() - 0.5) * 0.6;
      this.radius = Math.random() * 2 + 1;
      this.baseColor = Math.random() > 0.4 ? 'rgba(56, 189, 248, ' : 'rgba(99, 102, 241, ';
    }

    update() {
      this.x += this.vx;
      this.y += this.vy;

      if (this.x < 0 || this.x > width) this.vx *= -1;
      if (this.y < 0 || this.y > height) this.vy *= -1;

      // Mouse interactivity
      if (mouse.x !== null && mouse.y !== null) {
        const dx = mouse.x - this.x;
        const dy = mouse.y - this.y;
        const dist = Math.hypot(dx, dy);
        if (dist < mouse.radius) {
          const force = (mouse.radius - dist) / mouse.radius;
          const dirX = dx / dist;
          const dirY = dy / dist;
          this.x -= dirX * force * 2;
          this.y -= dirY * force * 2;
        }
      }
    }

    draw() {
      ctx.beginPath();
      ctx.arc(this.x, this.y, this.radius, 0, Math.PI * 2);
      ctx.fillStyle = this.baseColor + '0.6)';
      ctx.fill();
    }
  }

  function createParticles() {
    particles = [];
    const particleCount = Math.floor((width * height) / 16000);
    const count = Math.min(Math.max(particleCount, 40), 90);
    for (let i = 0; i < count; i++) {
      particles.push(new Particle());
    }
  }

  function connect() {
    const maxDistance = 130;
    for (let a = 0; a < particles.length; a++) {
      for (let b = a + 1; b < particles.length; b++) {
        const dx = particles[a].x - particles[b].x;
        const dy = particles[a].y - particles[b].y;
        const distance = Math.hypot(dx, dy);

        if (distance < maxDistance) {
          const opacity = 1 - distance / maxDistance;
          ctx.strokeStyle = `rgba(56, 189, 248, ${opacity * 0.18})`;
          ctx.lineWidth = 0.8;
          ctx.beginPath();
          ctx.moveTo(particles[a].x, particles[a].y);
          ctx.lineTo(particles[b].x, particles[b].y);
          ctx.stroke();
        }
      }
    }
  }

  function animate() {
    ctx.clearRect(0, 0, width, height);
    for (let i = 0; i < particles.length; i++) {
      particles[i].update();
      particles[i].draw();
    }
    connect();
    animationFrameId = requestAnimationFrame(animate);
  }

  resize();
  animate();
}

/* ==========================================================================
   3. Navigation, Active Spy, & Mobile Drawer
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

  // Active section spy on scroll
  window.addEventListener('scroll', () => {
    let current = '';
    const scrollY = window.pageYOffset;

    sections.forEach(section => {
      const sectionHeight = section.offsetHeight;
      const sectionTop = section.offsetTop - 120;
      const sectionId = section.getAttribute('id');

      if (scrollY >= sectionTop && scrollY < sectionTop + sectionHeight) {
        current = sectionId;
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
   4. Interactive Cockpit / Terminal Simulator (abhitvg@engine:~$ )
   ========================================================================== */
function initTerminal() {
  const input = document.getElementById('terminalInput');
  const output = document.getElementById('terminalOutput');
  const quickBtns = document.querySelectorAll('.quick-cmd-btn');
  let history = [];
  let historyIndex = -1;

  const commands = {
    'help': `Available Commands:
  <span class="highlight">cat profile.json</span>        — View full engineer background & education
  <span class="highlight">systemctl status arch</span>   — Production platforms uptime & telemetry
  <span class="highlight">kafka-topics --describe</span> — Event streaming & decoupled queue specs
  <span class="highlight">run benchmarks</span>          — Performance optimization & latency diffs
  <span class="highlight">curl /skills</span>            — Categorized tech stack index
  <span class="highlight">contact</span>                 — Reach out directly to Abhishek
  <span class="highlight">clear</span>                   — Clear terminal buffer`,

    'cat profile.json': `{
  "name": "Abhishek Singh",
  "title": "Software Development Engineer — Java Full Stack & Distributed Systems",
  "education": {
    "institution": "Jawaharlal Nehru University (JNU), New Delhi",
    "degree": "Dual Degree: B.Tech & M.Tech in Computer Science & Engineering",
    "gpa": "7.1 / 10",
    "duration": "2020 – 2025"
  },
  "thesis": "Social Network Analysis on Media Bias in News Articles (50,000+ NLP dataset)",
  "leadership": "Joint Secretary, JNU Alumni Association (2025–Present)",
  "competitive": "GATE CS/IT 2024 Qualified | JEE Mains AIR 6126",
  "github": "https://github.com/Abhitvg",
  "linkedin": "https://www.linkedin.com/in/abhisheksingh22141"
}`,

    'systemctl status arch': `● atma-production-cluster.service - Distributed Enterprise Cloud
     Loaded: loaded (/lib/systemd/system/cloud-orchestrator.service; enabled)
     Active: <span class="success">active (running)</span> since Jul 2025; 99.98% uptime
     Main PID: 1042 (java-spring-boot)
     Tasks: 128 (limit: 4096)
     Throughput: <span class="highlight">10,000+ Daily Active Users</span> across 3 core platforms
     Memory: 4.2G / 16.0G (Optimized GC G1GC)
     Status: "Zero-loss Kafka event ingestion, Solr 50k SKU search latency &lt;180ms"`,

    'kafka-topics --describe': `Topic: telemetry.events.inventory   PartitionCount: 6  ReplicationFactor: 3
Topic: user.promotions.realtime    PartitionCount: 4  ReplicationFactor: 3
Topic: rag.offline.documents       PartitionCount: 3  ReplicationFactor: 2
Consumer Group: [order-dispatch-service] -> Lag: <span class="success">0 msgs</span> (Event Pipeline active)
Consumer Group: [search-indexer-solr]    -> Lag: <span class="success">0 msgs</span> (Sync batch replaced)`,

    'run benchmarks': `[BENCHMARK RESULTS: BEFORE vs AFTER OPTIMIZATION]
1. MongoDB Query Latency (Compound Indexing):
   [BEFORE] avg: 142ms | [AFTER] avg: 92ms  -> <span class="success">35.2% Latency Reduction</span>
2. Solr Full-Text Search (50K-SKU Catalog):
   [BEFORE] avg: 480ms | [AFTER] avg: 165ms -> <span class="success">Click-through up 20%</span>
3. Oracle SQL Executive Report Execution Plans:
   [BEFORE] avg: 18.4s | [AFTER] avg: 11.0s -> <span class="success">40.2% Report Time Cut</span>
4. CI/CD Pipeline Automation:
   Post-release defects down <span class="success">45%</span> via Jenkins + Docker workflows.`,

    'curl /skills': `{
  "languages": ["Java 8/17+", "JavaScript ES6+", "TypeScript", "Python", "C++"],
  "backend": ["Spring Boot", "Spring MVC", "Spring Data", "Spring Security", "REST APIs", "Node.js"],
  "frontend": ["Angular 8+", "React.js", "Next.js", "Redux", "Tailwind CSS", "Three.js"],
  "data_systems": ["MongoDB", "PostgreSQL", "Oracle SQL", "Apache Kafka", "Apache Solr", "ActiveMQ"],
  "devops_testing": ["Docker", "Jenkins", "GitHub Actions", "Linux", "AWS", "JUnit", "Mockito", "Swagger"]
}`,

    'contact': `Email: <span class="highlight">Abhisheksingh1thakur@gmail.com</span>
Phone: <span class="highlight">+91-8850970898</span>
GitHub: <a href="https://github.com/Abhitvg" target="_blank" class="highlight">github.com/Abhitvg</a>
LinkedIn: <a href="https://www.linkedin.com/in/abhisheksingh22141" target="_blank" class="highlight">linkedin.com/in/abhisheksingh22141</a>
Type 'clear' or explore other commands!`,

    'whoami': 'Abhishek Singh — Software Development Engineer (Distributed Systems, Java Full Stack)',
    'clear': 'CLEAR'
  };

  function executeCommand(rawCmd) {
    const cmd = rawCmd.trim().toLowerCase();
    if (!cmd) return;

    history.push(rawCmd);
    historyIndex = history.length;

    if (cmd === 'clear') {
      output.innerHTML = '';
      return;
    }

    const commandBlock = document.createElement('div');
    commandBlock.className = 'terminal-output-block';

    const promptLine = document.createElement('div');
    promptLine.className = 'terminal-prompt-line';
    promptLine.innerHTML = `<span class="terminal-user">abhitvg</span><span class="terminal-at">@</span><span class="terminal-path">engine</span>:<span class="terminal-symbol">~$</span> <span class="terminal-command-text">${escapeHtml(rawCmd)}</span>`;
    commandBlock.appendChild(promptLine);

    const response = document.createElement('div');
    response.className = 'terminal-response';

    if (commands[cmd]) {
      response.innerHTML = commands[cmd];
    } else {
      // Fuzzy search or command not found
      let matchKey = Object.keys(commands).find(k => k.startsWith(cmd) || cmd.includes(k.split(' ')[0]));
      if (matchKey && cmd.length > 2) {
        response.innerHTML = `<span class="warn">Command '${escapeHtml(rawCmd)}' not found. Did you mean:</span> <span class="highlight">${matchKey}</span>?\nType '<span class="highlight">help</span>' for a list of commands.`;
      } else {
        response.innerHTML = `<span class="warn">zsh: command not found: ${escapeHtml(rawCmd)}</span>\nType '<span class="highlight">help</span>' to see valid operations.`;
      }
    }

    commandBlock.appendChild(response);
    output.appendChild(commandBlock);

    // Auto-scroll to bottom of terminal
    output.scrollTop = output.scrollHeight;
  }

  if (input) {
    input.addEventListener('keydown', (e) => {
      if (e.key === 'Enter') {
        const cmd = input.value;
        input.value = '';
        executeCommand(cmd);
      } else if (e.key === 'ArrowUp') {
        e.preventDefault();
        if (historyIndex > 0) {
          historyIndex--;
          input.value = history[historyIndex];
        }
      } else if (e.key === 'ArrowDown') {
        e.preventDefault();
        if (historyIndex < history.length - 1) {
          historyIndex++;
          input.value = history[historyIndex];
        } else {
          historyIndex = history.length;
          input.value = '';
        }
      }
    });
  }

  // Quick command buttons
  quickBtns.forEach(btn => {
    btn.addEventListener('click', () => {
      const cmd = btn.getAttribute('data-cmd');
      if (cmd) {
        executeCommand(cmd);
      }
    });
  });

  function escapeHtml(str) {
    return str.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
  }
}

/* ==========================================================================
   5. Project Filtering
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
          card.style.opacity = '0';
          setTimeout(() => {
            card.style.opacity = '1';
          }, 50);
        } else {
          card.style.display = 'none';
        }
      });
    });
  });
}

/* ==========================================================================
   6. Project Detailed Case Studies & Native Dialog Modal
   ========================================================================== */
const projectData = {
  'porcupine': {
    title: 'Porcupine – Local-First AI Orchestrator',
    subtitle: 'Multi-agent ERP desktop application with secure offline RAG',
    date: 'February 2026',
    category: 'AI & Distributed',
    image: 'assets/project-porcupine.png',
    github: 'https://github.com/Abhitvg/porcupine',
    demo: '#',
    techStack: ['Electron', 'Node.js', 'Next.js', 'SQLite (Encrypted)', 'ONNX Runtime', 'Xenova/Transformers', 'Tailwind CSS'],
    overview: 'Porcupine is an enterprise-grade desktop orchestrator designed for offline-first AI workflows, strict privacy governance, and multi-agent ERP management. It eliminates reliance on public cloud APIs for sensitive corporate data retrieval.',
    architecturalHighlights: [
      'Engineered an Electron-based multi-agent supervisor system with automatic LLM provider failover and an autonomous AI critique loop for output verification.',
      'Constructed a 100% local vector retrieval pipeline leveraging ONNX Runtime and Xenova browser/node transformers, delivering sub-50ms local semantic embeddings.',
      'Architected an encrypted SQLite storage engine with SQLCipher for confidential CRM, financial ledgers, and automated publishing workflows.',
      'Engineered a real-time reactive Next.js executive cockpit with IPC event streaming to monitor agent health, task queues, and memory usage.'
    ]
  },
  'distributed-backend': {
    title: 'ATMA High-Throughput Distributed Microservices Hub',
    subtitle: 'Spring Boot 3, Kafka event queues, Solr search & Mongo optimization',
    date: 'Jul 2025 – Present',
    category: 'Distributed & Cloud',
    image: 'assets/project-distributed.jpg',
    github: 'https://github.com/Abhitvg/ATMA-AI-Backend',
    demo: '#',
    techStack: ['Java 8/17', 'Spring Boot', 'Apache Kafka', 'Apache Solr', 'MongoDB', 'Oracle SQL', 'JUnit', 'Mockito'],
    overview: 'Production backend architecture powering three platforms serving 10,000+ daily active users with 99.9% uptime. Designed for continuous zero-downtime microservice communication and high-load catalog search.',
    architecturalHighlights: [
      'Engineered Kafka-based event streaming pipelines that replaced legacy hourly cron batches, providing immediate consistency for live inventory and promotion updates.',
      'Re-indexed MongoDB collections and designed compound covered indexes, slashing average read latency by 35%.',
      'Deployed Apache Solr with custom phonetic tokenizers and faceted filters across 50,000+ catalog items, driving a 20% increase in search conversion.',
      'Rewrote Oracle SQL analytical execution plans and implemented materialized views for C-level dashboards, trimming generation time by 40%.',
      'Enforced comprehensive unit and integration testing with JUnit and Mockito, ensuring 85%+ test coverage across all microservice contracts.'
    ]
  },
  'virasat': {
    title: 'CIET-NCERT Virasat – Cultural Heritage Explorer',
    subtitle: 'PAC Project 20.09: Survey of India SVG drill-down engine & 3D artifacts',
    date: 'Nov 2025 – Jun 2026',
    category: 'Frontend & 3D',
    image: 'assets/project-virasat.webp',
    github: 'https://github.com/Abhitvg/bharat-heritage',
    demo: '#',
    techStack: ['React.js', '@react-three/fiber', 'Three.js', 'TopoJSON', 'SVG Drill-Down Engine', 'Local JSON Pipeline'],
    overview: 'A high-impact national heritage education platform developed for CIET-NCERT to enable millions of students across India to explore cultural artifacts and geographic landmarks with zero network latency.',
    architecturalHighlights: [
      'Engineered a 100% local JSON data pipeline, eliminating external network round-trips and making the platform resilient even in low-connectivity rural school labs.',
      'Architected a Survey of India-compliant SVG drill-down map engine featuring three-tier zoom logic (Country → State → District heritage nodes).',
      'Integrated @react-three/fiber and Three.js for interactive, photorealistic 3D artifact rendering directly in browser canvas with WebGL hardware acceleration.',
      'Optimized TopoJSON geometric boundaries and conducted automated load simulations, achieving 60fps rendering performance on low-spec hardware.'
    ]
  },
  'media-bias': {
    title: 'Computational NLP Media Bias Detection Engine',
    subtitle: 'M.Tech Thesis at JNU: Social Network Analysis across 50,000+ articles',
    date: '2024 – 2025',
    category: 'AI & Data Science',
    image: 'assets/project-thesis.jpg',
    github: 'https://github.com/Abhitvg/Media-Bias-Analysis-in-Indian-News-Articles',
    demo: '#',
    techStack: ['Python', 'NLP', 'NetworkX', 'Graph Data Science', 'Topic Modeling', 'Pandas', 'BERT Embeddings'],
    overview: 'Master of Technology research thesis exploring computational media bias and agenda-setting in major Indian news publications. Processed a massive corpus of 50,000+ news articles with automated NLP entity extraction.',
    architecturalHighlights: [
      'Built a distributed NLP scraping and ingestion pipeline that normalized over 50,000 news articles from top national digital dailies.',
      'Constructed social co-occurrence networks using NetworkX, revealing hidden framing topologies, sentiment skew, and entity centrality metrics.',
      'Implemented unsupervised topic modeling (LDA & BERTopic) combined with transformer sentiment scoring to quantify political and socioeconomic framing.',
      'Defended thesis successfully before the JNU School of Computer and Systems Sciences committee with distinction.'
    ]
  },
  'zubin-mehta': {
    title: 'Embassy of Israel – Zubin Mehta Cultural Portal',
    subtitle: 'Interactive frontend portal commemorating Maestro Zubin Mehta',
    date: 'May 2026',
    category: 'Frontend & Web',
    image: 'assets/project-zubin.jpg',
    github: 'https://github.com/Abhitvg/zubin-mehta-project',
    demo: '#',
    techStack: ['React.js', 'Modern CSS', 'State Management', 'Lazy Loading', 'Responsive Asset Pipelines'],
    overview: 'An interactive cultural initiative portal developed for the Embassy of Israel honoring the legendary conductor Zubin Mehta, showcasing orchestral archives, interactive timeline, and visual retrospectives.',
    architecturalHighlights: [
      'Architected modular React component hierarchy with fine-grained state management to orchestrate high-resolution visual storytelling.',
      'Implemented progressive image loading, WebP asset compression, and lazy-loading wrappers to ensure instant First Contentful Paint (FCP).',
      'Designed responsive, accessible audio/visual galleries with keyboard navigation support compliant with WCAG 2.1 AA standards.'
    ]
  },
  'doonsilk': {
    title: 'Uttarakhand Shashan Co-Operative / DoonSilk',
    subtitle: 'Real-time supply chain networking platform for artisan sericulture',
    date: 'Jan 2024',
    category: 'Full Stack & Web',
    image: 'assets/project-doonsilk.jpg',
    github: 'https://github.com/Abhitvg/uk-coop-main',
    demo: '#',
    techStack: ['MERN Stack', 'Next.js', 'MongoDB', 'Express', 'Node.js', 'REST APIs'],
    overview: 'A digital transformation platform built for state artisan cooperatives, streamlining the supply chain from raw cocoon harvesting to handloom distribution and direct consumer ordering.',
    architecturalHighlights: [
      'Engineered normalized MongoDB NoSQL schemas for multi-tier product inventories, batch traceability, and artisan cooperative payouts.',
      'Developed RESTful endpoints with role-based JWT authorization for regional inspectors, warehouse managers, and retail distributors.',
      'Built reactive Next.js interfaces with real-time stock alert webhooks, cutting inventory reconciliation lag from days to seconds.'
    ]
  },
  'ierf': {
    title: 'Indian Eurasia Research Forum (IERF) Infrastructure',
    subtitle: 'Digital infrastructure for international geopolitics & research dissemination',
    date: 'Oct 2025 – Jul 2026',
    category: 'Full Stack & Web',
    image: 'assets/project-ierf.png',
    github: 'https://github.com/Abhitvg/india-eurasia-research-forum',
    demo: '#',
    techStack: ['Next.js', 'TypeScript', 'Cloud Hosting', 'SEO Optimization', 'CI/CD Pipelines'],
    overview: 'Led technical initiatives as Technical Lead (Part-time), architecting digital publication and repository infrastructure for global research scholars, diplomatic think tanks, and policy analysis.',
    architecturalHighlights: [
      'Orchestrated multi-region Next.js deployment with automated incremental static regeneration (ISR) for fast international page loads.',
      'Implemented automated document indexing and metadata pipelines for peer-reviewed working papers and monographs.',
      'Established GitHub Actions CI/CD workflows for automated accessibility audits, link verification, and security scanning.'
    ]
  }
};

function initProjectModal() {
  const modal = document.getElementById('projectModal');
  const modalCloseBtn = document.getElementById('modalCloseBtn');
  const projectCards = document.querySelectorAll('.project-card');

  if (!modal) return;

  projectCards.forEach(card => {
    card.addEventListener('click', () => {
      const projectId = card.getAttribute('data-project');
      const data = projectData[projectId];
      if (!data) return;

      document.getElementById('modalImg').src = data.image;
      document.getElementById('modalImg').alt = data.title;
      document.getElementById('modalCategory').textContent = data.category;
      document.getElementById('modalDate').textContent = data.date;
      document.getElementById('modalTitle').textContent = data.title;
      document.getElementById('modalSubtitle').textContent = data.subtitle;
      document.getElementById('modalOverview').textContent = data.overview;

      // Populate highlights
      const highlightsContainer = document.getElementById('modalHighlights');
      highlightsContainer.innerHTML = '';
      data.architecturalHighlights.forEach(item => {
        const li = document.createElement('li');
        li.textContent = item;
        highlightsContainer.appendChild(li);
      });

      // Populate tech stack
      const techContainer = document.getElementById('modalTech');
      techContainer.innerHTML = '';
      data.techStack.forEach(t => {
        const span = document.createElement('span');
        span.className = 'tech-chip';
        span.textContent = t;
        techContainer.appendChild(span);
      });

      // Links
      const githubLink = document.getElementById('modalGithubLink');
      if (githubLink) {
        githubLink.href = data.github;
      }

      modal.showModal();
      document.body.style.overflow = 'hidden';
    });
  });

  function closeModal() {
    modal.close();
    document.body.style.overflow = '';
  }

  if (modalCloseBtn) {
    modalCloseBtn.addEventListener('click', closeModal);
  }

  // Light dismiss: click outside modal backdrop
  modal.addEventListener('click', (e) => {
    const dialogDimensions = modal.getBoundingClientRect();
    if (
      e.clientX < dialogDimensions.left ||
      e.clientX > dialogDimensions.right ||
      e.clientY < dialogDimensions.top ||
      e.clientY > dialogDimensions.bottom
    ) {
      closeModal();
    }
  });

  modal.addEventListener('cancel', () => {
    document.body.style.overflow = '';
  });
}

/* ==========================================================================
   7. One-Click Copy Buttons & Toast Notifications
   ========================================================================== */
function initCopyButtons() {
  const copyBtns = document.querySelectorAll('.copy-btn');

  copyBtns.forEach(btn => {
    btn.addEventListener('click', (e) => {
      e.stopPropagation();
      const textToCopy = btn.getAttribute('data-copy');
      if (!textToCopy) return;

      navigator.clipboard.writeText(textToCopy).then(() => {
        const originalIcon = btn.innerHTML;
        btn.innerHTML = '<i class="fa-solid fa-check" style="color: #10b981;"></i>';
        showToast(`Copied to clipboard: ${textToCopy}`, 'success');

        setTimeout(() => {
          btn.innerHTML = originalIcon;
        }, 2000);
      }).catch(err => {
        console.error('Copy failed', err);
        showToast('Failed to copy to clipboard', 'error');
      });
    });
  });
}

function showToast(message, type = 'info') {
  let container = document.querySelector('.toast-container');
  if (!container) {
    container = document.createElement('div');
    container.className = 'toast-container';
    document.body.appendChild(container);
  }

  const toast = document.createElement('div');
  toast.className = 'toast';

  let icon = 'fa-circle-info';
  if (type === 'success') icon = 'fa-circle-check';
  if (type === 'error') icon = 'fa-circle-exclamation';

  toast.innerHTML = `<i class="fa-solid ${icon}"></i> <span>${message}</span>`;
  container.appendChild(toast);

  // Trigger animation
  setTimeout(() => toast.classList.add('show'), 20);

  // Dismiss
  setTimeout(() => {
    toast.classList.remove('show');
    setTimeout(() => toast.remove(), 300);
  }, 3500);
}

/* ==========================================================================
   8. Contact Form Interaction
   ========================================================================== */
function initContactForm() {
  const form = document.getElementById('contactForm');
  if (!form) return;

  form.addEventListener('submit', (e) => {
    e.preventDefault();
    const name = document.getElementById('formName').value.trim();
    const email = document.getElementById('formEmail').value.trim();
    const subject = document.getElementById('formSubject').value.trim();
    const message = document.getElementById('formMessage').value.trim();

    if (!name || !email || !message) {
      showToast('Please fill in all required fields.', 'error');
      return;
    }

    const mailtoSubject = encodeURIComponent(subject ? `Portfolio Inquiry: ${subject}` : `Inquiry from ${name}`);
    const mailtoBody = encodeURIComponent(`Sender: ${name}\nEmail: ${email}\n\nMessage:\n${message}`);
    const mailtoUrl = `mailto:Abhisheksingh1thakur@gmail.com?subject=${mailtoSubject}&body=${mailtoBody}`;

    window.location.href = mailtoUrl;
    showToast('Opening your email client...', 'success');
    form.reset();
  });
}

/* ==========================================================================
   9. Live Clock for Local Time (IST / New Delhi)
   ========================================================================== */
function initLiveClock() {
  const clockElement = document.getElementById('liveClock');
  if (!clockElement) return;

  function update() {
    const options = {
      timeZone: 'Asia/Kolkata',
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit',
      hour12: true
    };
    const timeStr = new Intl.DateTimeFormat([], options).format(new Date());
    clockElement.textContent = `${timeStr} (IST, New Delhi)`;
  }

  update();
  setInterval(update, 1000);
}
