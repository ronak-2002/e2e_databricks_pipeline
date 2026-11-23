/**
 * PulseStore - Vanilla JavaScript E-Commerce & Kafka Stream Client
 * No build tools, no frontend frameworks, pure vanilla fetch() API.
 */

(function () {
  "use strict";

  // Application State
  const state = {
    session: {
      session_id: "sess_init_" + Math.random().toString(36).substring(2, 10),
      user_id: "usr_init_" + Math.random().toString(36).substring(2, 8),
      device_type: "web",
      name: "Elena Rostova",
      avatar: "👩‍💻",
      email: "elena.rostova@example.com",
    },
    deviceType: "web",
    personas: [],
    catalog: [],
    categories: [],
    currentCategory: "all",
    searchQuery: "",
    cart: { items: [], subtotal: 0, item_count: 0 },
    latestSeq: 0,
    isStreamPaused: false,
    simulatorRunning: false,
    searchDebounceTimer: null,
  };

  // DOM Elements Cache
  const DOM = {
    kafkaStatusBadge: document.getElementById("kafkaStatusBadge"),
    kafkaModeText: document.getElementById("kafkaModeText"),
    personaSelect: document.getElementById("personaSelect"),
    deviceBtns: document.querySelectorAll(".device-btn"),
    openCartBtn: document.getElementById("openCartBtn"),
    cartBadgeCount: document.getElementById("cartBadgeCount"),
    cartBadgeTotal: document.getElementById("cartBadgeTotal"),

    searchInput: document.getElementById("searchInput"),
    clearSearchBtn: document.getElementById("clearSearchBtn"),
    categoryTabs: document.getElementById("categoryTabs"),
    productCountLabel: document.getElementById("productCountLabel"),
    productGrid: document.getElementById("productGrid"),

    simPulseIndicator: document.getElementById("simPulseIndicator"),
    simToggleBtn: document.getElementById("simToggleBtn"),
    simBotsRange: document.getElementById("simBotsRange"),
    simBotsVal: document.getElementById("simBotsVal"),
    simSpeedRange: document.getElementById("simSpeedRange"),
    simSpeedVal: document.getElementById("simSpeedVal"),
    simTotalTicks: document.getElementById("simTotalTicks"),
    botsStatusGrid: document.getElementById("botsStatusGrid"),

    totalEventsBadge: document.getElementById("totalEventsBadge"),
    pauseStreamBtn: document.getElementById("pauseStreamBtn"),
    clearBufferBtn: document.getElementById("clearBufferBtn"),
    resetStateBtn: document.getElementById("resetStateBtn"),
    filterTopic: document.getElementById("filterTopic"),
    filterSource: document.getElementById("filterSource"),
    bufferUsageText: document.getElementById("bufferUsageText"),
    topicMetricsBar: document.getElementById("topicMetricsBar"),
    eventFeedContainer: document.getElementById("eventFeedContainer"),
    feedEmptyState: document.getElementById("feedEmptyState"),

    productModal: document.getElementById("productModal"),
    modalProdTitle: document.getElementById("modalProdTitle"),
    modalProdBody: document.getElementById("modalProdBody"),
    closeProdModalBtn: document.getElementById("closeProdModalBtn"),

    cartModal: document.getElementById("cartModal"),
    cartItemsList: document.getElementById("cartItemsList"),
    cartSummarySubtotal: document.getElementById("cartSummarySubtotal"),
    cartSummaryTax: document.getElementById("cartSummaryTax"),
    cartSummaryShipping: document.getElementById("cartSummaryShipping"),
    cartSummaryTotal: document.getElementById("cartSummaryTotal"),
    closeCartModalBtn: document.getElementById("closeCartModalBtn"),
    clearCartBtn: document.getElementById("clearCartBtn"),
    proceedCheckoutBtn: document.getElementById("proceedCheckoutBtn"),

    checkoutModal: document.getElementById("checkoutModal"),
    checkoutForm: document.getElementById("checkoutForm"),
    checkoutReviewSection: document.getElementById("checkoutReviewSection"),
    closeCheckoutModalBtn: document.getElementById("closeCheckoutModalBtn"),
    cancelCheckoutBtn: document.getElementById("cancelCheckoutBtn"),

    orderConfirmModal: document.getElementById("orderConfirmModal"),
    orderConfirmContent: document.getElementById("orderConfirmContent"),
    closeConfirmModalBtn: document.getElementById("closeConfirmModalBtn"),
  };

  // ---------------------------------------------------------------------------
  // Helper Network Utilities
  // ---------------------------------------------------------------------------
  function getRequestHeaders() {
    return {
      "Content-Type": "application/json",
      "X-Session-ID": state.session.session_id,
      "X-User-ID": state.session.user_id,
      "X-Device-Type": state.deviceType,
    };
  }

  async function apiFetch(url, options = {}) {
    const defaultHeaders = getRequestHeaders();
    const mergedHeaders = { ...defaultHeaders, ...(options.headers || {}) };
    const response = await fetch(url, { ...options, headers: mergedHeaders });
    if (!response.ok) {
      const errorData = await response.json().catch(() => ({}));
      throw new Error(errorData.error || `HTTP error ${response.status}`);
    }
    return response.json();
  }

  // ---------------------------------------------------------------------------
  // Initialization
  // ---------------------------------------------------------------------------
  async function initApp() {
    setupEventListeners();
    await loadConfig();
    await loadPersonas();
    await initSession();
    await loadCatalog();
    await refreshCart();

    // Start background event stream and metrics pollers
    setInterval(pollLiveEvents, 1000);
    setInterval(pollTelemetryStats, 1500);
  }

  async function loadConfig() {
    try {
      const res = await apiFetch("/api/config");
      if (res.success && res.config) {
        const mode = res.config.kafka_mode || "MOCK";
        DOM.kafkaModeText.textContent = `Kafka: ${mode}`;
        if (mode === "MOCK") {
          DOM.kafkaStatusBadge.title = "Local Mock Mode: Events recorded locally in-memory";
        } else {
          DOM.kafkaStatusBadge.title = `Connected to ${res.config.kafka_bootstrap_servers}`;
        }
      }
    } catch (e) {
      console.warn("Failed to load config:", e);
    }
  }

  async function loadPersonas() {
    try {
      const res = await apiFetch("/api/personas");
      if (res.success && res.personas) {
        state.personas = res.personas;
        DOM.personaSelect.innerHTML = res.personas
          .map(
            (p) =>
              `<option value="${p.user_id}">${p.avatar} ${p.name} (${p.preferred_category})</option>`
          )
          .join("");
      }
    } catch (e) {
      console.warn("Failed to load personas:", e);
    }
  }

  async function initSession() {
    try {
      const firstPersona = state.personas[0];
      const res = await apiFetch("/api/session", {
        method: "POST",
        body: JSON.stringify({
          user_id: firstPersona ? firstPersona.user_id : state.session.user_id,
          device_type: state.deviceType,
          user_meta: firstPersona || {},
        }),
      });
      if (res.success) {
        state.session = res.session;
        if (res.cart) updateCartUI(res.cart);
      }
    } catch (e) {
      console.warn("Session init error:", e);
    }
  }

  // ---------------------------------------------------------------------------
  // Catalog Management & Rendering
  // ---------------------------------------------------------------------------
  async function loadCatalog() {
    try {
      let url = "/api/catalog";
      const params = new URLSearchParams();
      if (state.currentCategory && state.currentCategory !== "all") {
        params.append("category", state.currentCategory);
      }
      if (state.searchQuery && state.searchQuery.trim()) {
        params.append("search", state.searchQuery.trim());
      }
      if (params.toString()) {
        url += `?${params.toString()}`;
      }

      const res = await apiFetch(url);
      if (res.success) {
        state.catalog = res.products;
        state.categories = res.categories;
        renderCatalog();
      }
    } catch (e) {
      console.error("Failed to load catalog:", e);
    }
  }

  function renderCatalog() {
    DOM.productCountLabel.textContent = `Showing ${state.catalog.length} products`;

    if (state.catalog.length === 0) {
      DOM.productGrid.innerHTML = `
        <div style="grid-column: 1 / -1; text-align: center; padding: 40px; color: var(--text-dim);">
          <div style="font-size: 36px; margin-bottom: 8px;">🔍</div>
          <p>No products match your criteria.</p>
        </div>
      `;
      return;
    }

    DOM.productGrid.innerHTML = state.catalog
      .map(
        (prod) => `
        <div class="product-card" data-id="${prod.id}">
          <div class="card-top">
            <span class="prod-category-badge">${escapeHtml(prod.category)}</span>
            <span class="prod-rating">★ ${prod.rating} <span>(${prod.reviews_count})</span></span>
          </div>
          <div class="prod-icon">${prod.image_icon || "📦"}</div>
          <h4 class="prod-title" title="${escapeHtml(prod.name)}">${escapeHtml(prod.name)}</h4>
          <div class="card-bottom">
            <div class="prod-price">$${prod.price.toFixed(2)}</div>
            <button type="button" class="btn-add-card" data-action="add-cart" data-id="${prod.id}">
              + Add to Cart
            </button>
          </div>
        </div>
      `
      )
      .join("");
  }

  // ---------------------------------------------------------------------------
  // Shopping Cart Operations
  // ---------------------------------------------------------------------------
  async function refreshCart() {
    try {
      const res = await apiFetch(`/api/cart?session_id=${encodeURIComponent(state.session.session_id)}`);
      if (res.success && res.cart) {
        updateCartUI(res.cart);
      }
    } catch (e) {
      console.warn("Cart fetch error:", e);
    }
  }

  function updateCartUI(cart) {
    state.cart = cart;
    DOM.cartBadgeCount.textContent = cart.item_count || 0;
    DOM.cartBadgeTotal.textContent = `$${(cart.subtotal || 0).toFixed(2)}`;

    // Render cart items inside modal
    if (!cart.items || cart.items.length === 0) {
      DOM.cartItemsList.innerHTML = `
        <div style="text-align: center; padding: 24px; color: var(--text-dim);">
          🛒 Your cart is currently empty.
        </div>
      `;
      DOM.proceedCheckoutBtn.disabled = true;
    } else {
      DOM.proceedCheckoutBtn.disabled = false;
      DOM.cartItemsList.innerHTML = cart.items
        .map(
          (item) => `
          <div class="cart-item-row">
            <div class="cart-item-info">
              <span class="cart-item-icon">${item.image_icon || "📦"}</span>
              <div>
                <div class="cart-item-name">${escapeHtml(item.name)}</div>
                <div class="cart-item-price">$${item.price.toFixed(2)} &times; ${item.quantity} = $${item.total.toFixed(2)}</div>
              </div>
            </div>
            <div class="cart-item-actions">
              <button type="button" class="btn-qty" data-action="dec-qty" data-id="${item.product_id}">-</button>
              <span><strong>${item.quantity}</strong></span>
              <button type="button" class="btn-qty" data-action="inc-qty" data-id="${item.product_id}">+</button>
              <button type="button" class="btn-qty btn-danger" data-action="del-item" data-id="${item.product_id}" title="Remove">&times;</button>
            </div>
          </div>
        `
        )
        .join("");
    }

    // Update summary lines
    const subtotal = cart.subtotal || 0;
    const tax = roundTo2(subtotal * 0.08);
    const shipping = subtotal >= 100.0 || subtotal === 0 ? 0.0 : 9.99;
    const total = roundTo2(subtotal + tax + shipping);

    DOM.cartSummarySubtotal.textContent = `$${subtotal.toFixed(2)}`;
    DOM.cartSummaryTax.textContent = `$${tax.toFixed(2)}`;
    DOM.cartSummaryShipping.textContent =
      shipping === 0 ? "FREE (Orders $100+)" : `$${shipping.toFixed(2)}`;
    DOM.cartSummaryTotal.textContent = `$${total.toFixed(2)}`;
  }

  async function handleAddToCart(productId, quantity = 1) {
    try {
      const res = await apiFetch("/api/cart/add", {
        method: "POST",
        body: JSON.stringify({
          session_id: state.session.session_id,
          user_id: state.session.user_id,
          device_type: state.deviceType,
          product_id: productId,
          quantity: quantity,
        }),
      });
      if (res.success) {
        updateCartUI(res.cart);
        flashCartBadge();
      }
    } catch (e) {
      alert("Failed to add item to cart: " + e.message);
    }
  }

  async function handleRemoveFromCart(productId, quantity = null) {
    try {
      const res = await apiFetch("/api/cart/remove", {
        method: "POST",
        body: JSON.stringify({
          session_id: state.session.session_id,
          user_id: state.session.user_id,
          device_type: state.deviceType,
          product_id: productId,
          quantity: quantity,
        }),
      });
      if (res.success) {
        updateCartUI(res.cart);
      }
    } catch (e) {
      console.error("Cart item remove error:", e);
    }
  }

  async function handleClearCart() {
    try {
      const res = await apiFetch("/api/cart/clear", {
        method: "POST",
        body: JSON.stringify({
          session_id: state.session.session_id,
          user_id: state.session.user_id,
          device_type: state.deviceType,
          reason: "user_clicked_clear_cart",
        }),
      });
      if (res.success) {
        updateCartUI(res.cart);
      }
    } catch (e) {
      console.error("Clear cart error:", e);
    }
  }

  function flashCartBadge() {
    DOM.openCartBtn.style.transform = "scale(1.08)";
    setTimeout(() => {
      DOM.openCartBtn.style.transform = "none";
    }, 200);
  }

  // ---------------------------------------------------------------------------
  // Product Detail Modal & View Event
  // ---------------------------------------------------------------------------
  async function openProductDetailModal(productId) {
    try {
      // Emit product_viewed event
      const res = await apiFetch("/api/events/view", {
        method: "POST",
        body: JSON.stringify({
          session_id: state.session.session_id,
          user_id: state.session.user_id,
          device_type: state.deviceType,
          product_id: productId,
        }),
      });

      if (res.success && res.product) {
        const p = res.product;
        DOM.modalProdTitle.textContent = p.name;
        DOM.modalProdBody.innerHTML = `
          <div style="display: flex; gap: 16px; align-items: flex-start; margin-bottom: 16px;">
            <div style="font-size: 54px; padding: 12px; background: var(--bg-card); border-radius: var(--radius-md);">${p.image_icon || "📦"}</div>
            <div>
              <div style="font-size: 11px; color: #93c5fd; text-transform: uppercase; font-weight: 700;">${escapeHtml(p.category)}</div>
              <h3 style="font-size: 18px; margin: 4px 0 8px;">${escapeHtml(p.name)}</h3>
              <div style="font-size: 22px; font-weight: 800; color: #34d399;">$${p.price.toFixed(2)}</div>
              <div style="font-size: 12px; color: #f59e0b; margin-top: 4px;">★ ${p.rating} / 5.0 (${p.reviews_count} reviews)</div>
            </div>
          </div>
          <p style="color: var(--text-muted); font-size: 13px; margin-bottom: 16px;">${escapeHtml(p.description)}</p>
          <div style="display: flex; gap: 8px; flex-wrap: wrap; margin-bottom: 20px;">
            ${(p.tags || []).map((t) => `<span style="background: var(--bg-card); font-size: 11px; padding: 3px 8px; border-radius: 4px; color: var(--text-dim);">#${escapeHtml(t)}</span>`).join("")}
          </div>
          <div style="display: flex; justify-content: space-between; align-items: center; border-top: 1px solid var(--border-color); padding-top: 14px;">
            <span style="font-size: 12px; color: ${p.stock > 15 ? "#34d399" : "#f59e0b"};">In Stock (${p.stock} units)</span>
            <button type="button" class="btn btn-primary" id="modalAddToCartBtn" data-id="${p.id}">
              + Add to Cart ($${p.price.toFixed(2)})
            </button>
          </div>
        `;

        document.getElementById("modalAddToCartBtn").addEventListener("click", () => {
          handleAddToCart(p.id, 1);
          closeModal(DOM.productModal);
        });

        openModal(DOM.productModal);
      }
    } catch (e) {
      console.error("Failed to view product:", e);
    }
  }

  // ---------------------------------------------------------------------------
  // Checkout & Order Completion
  // ---------------------------------------------------------------------------
  async function openCheckoutModal() {
    closeModal(DOM.cartModal);
    if (!state.cart.items || state.cart.items.length === 0) {
      alert("Cart is empty!");
      return;
    }

    try {
      // Emit checkout_started event
      await apiFetch("/api/checkout/start", {
        method: "POST",
        body: JSON.stringify({
          session_id: state.session.session_id,
          user_id: state.session.user_id,
          device_type: state.deviceType,
        }),
      });

      // Render checkout review section
      const subtotal = state.cart.subtotal;
      const tax = roundTo2(subtotal * 0.08);
      const shipping = subtotal >= 100.0 ? 0.0 : 9.99;
      const total = roundTo2(subtotal + tax + shipping);

      DOM.checkoutReviewSection.innerHTML = `
        <div style="background: var(--bg-card); padding: 10px; border-radius: var(--radius-sm); font-size: 12px; margin-top: 12px;">
          <div style="display: flex; justify-content: space-between; margin-bottom: 4px;">
            <span>Items (${state.cart.item_count}):</span>
            <strong>$${subtotal.toFixed(2)}</strong>
          </div>
          <div style="display: flex; justify-content: space-between; margin-bottom: 4px;">
            <span>Tax + Shipping:</span>
            <span>$${(tax + shipping).toFixed(2)}</span>
          </div>
          <div style="display: flex; justify-content: space-between; font-size: 14px; font-weight: bold; color: #34d399; border-top: 1px solid var(--border-color); padding-top: 6px; margin-top: 6px;">
            <span>Total to Pay:</span>
            <span>$${total.toFixed(2)}</span>
          </div>
        </div>
      `;

      openModal(DOM.checkoutModal);
    } catch (e) {
      alert("Checkout start failed: " + e.message);
    }
  }

  async function handleCheckoutSubmit(e) {
    e.preventDefault();
    const address = document.getElementById("shipAddress").value;
    const city = document.getElementById("shipCity").value;
    const stateVal = document.getElementById("shipState").value;
    const zip = document.getElementById("shipZip").value;
    const paymentMethod = document.querySelector('input[name="payMethod"]:checked').value;

    try {
      const res = await apiFetch("/api/checkout/complete", {
        method: "POST",
        body: JSON.stringify({
          session_id: state.session.session_id,
          user_id: state.session.user_id,
          device_type: state.deviceType,
          shipping: { address, city, state: stateVal, zip },
          payment: { method: paymentMethod, last4: "4242" },
        }),
      });

      if (res.success && res.order) {
        closeModal(DOM.checkoutModal);
        refreshCart();

        // Show confirmation modal
        DOM.orderConfirmContent.innerHTML = `
          <div class="order-success-icon">🎉</div>
          <h3>Order Placed Successfully!</h3>
          <div class="order-id-tag">Order ID: ${res.order.order_id}</div>
          <p style="color: var(--text-muted); font-size: 12px; margin-bottom: 12px;">
            Events <strong>payment_processed</strong> and <strong>order_placed</strong> were emitted to Kafka topic <code>ecom.orders</code>.
          </p>
          <div style="background: var(--bg-card); padding: 12px; border-radius: var(--radius-sm); text-align: left; font-size: 12px;">
            <div><strong>Total Paid:</strong> $${res.order.total.toFixed(2)}</div>
            <div><strong>Items:</strong> ${res.order.items.length} product(s)</div>
            <div><strong>Shipping to:</strong> ${escapeHtml(city)}, ${escapeHtml(stateVal)}</div>
          </div>
        `;
        openModal(DOM.orderConfirmModal);
      }
    } catch (err) {
      alert("Checkout error: " + err.message);
    }
  }

  // ---------------------------------------------------------------------------
  // Traffic Simulator Controls
  // ---------------------------------------------------------------------------
  async function toggleSimulator() {
    try {
      if (state.simulatorRunning) {
        const res = await apiFetch("/api/simulator/stop", { method: "POST" });
        if (res.success) {
          state.simulatorRunning = false;
          updateSimulatorUI(res.simulator);
        }
      } else {
        const res = await apiFetch("/api/simulator/start", {
          method: "POST",
          body: JSON.stringify({
            num_users: parseInt(DOM.simBotsRange.value, 10),
            speed: parseFloat(DOM.simSpeedRange.value),
          }),
        });
        if (res.success) {
          state.simulatorRunning = true;
          updateSimulatorUI(res.simulator);
        }
      }
    } catch (e) {
      alert("Simulator control error: " + e.message);
    }
  }

  async function updateSimulatorConfig() {
    const numBots = parseInt(DOM.simBotsRange.value, 10);
    const speed = parseFloat(DOM.simSpeedRange.value);
    DOM.simBotsVal.textContent = numBots;
    DOM.simSpeedVal.textContent = `${speed.toFixed(1)}s`;

    try {
      const res = await apiFetch("/api/simulator/config", {
        method: "POST",
        body: JSON.stringify({ num_users: numBots, speed: speed }),
      });
      if (res.success) {
        updateSimulatorUI(res.simulator);
      }
    } catch (e) {
      console.warn("Simulator config update error:", e);
    }
  }

  function updateSimulatorUI(sim) {
    if (!sim) return;
    state.simulatorRunning = !!sim.is_running;

    if (sim.is_running) {
      DOM.simToggleBtn.textContent = "⏹ Stop Traffic";
      DOM.simToggleBtn.classList.remove("btn-primary");
      DOM.simToggleBtn.classList.add("btn-danger");
      DOM.simPulseIndicator.classList.add("running");
    } else {
      DOM.simToggleBtn.textContent = "▶ Start Traffic";
      DOM.simToggleBtn.classList.remove("btn-danger");
      DOM.simToggleBtn.classList.add("btn-primary");
      DOM.simPulseIndicator.classList.remove("running");
    }

    DOM.simTotalTicks.textContent = `Ticks: ${sim.total_ticks || 0}`;

    // Render virtual bot chips
    if (sim.bots && sim.bots.length > 0) {
      DOM.botsStatusGrid.innerHTML = sim.bots
        .map(
          (b) => `
          <div class="bot-chip">
            <div class="bot-chip-top">
              <span>${b.avatar || "🤖"} <strong>${escapeHtml(b.name.split(" ")[0])}</strong></span>
              <span class="bot-state-badge">${b.state}</span>
            </div>
            <div class="bot-action-text" title="${escapeHtml(b.last_action || "")}">
              ${escapeHtml(b.last_action || "Idle")}
            </div>
          </div>
        `
        )
        .join("");
    }
  }

  // ---------------------------------------------------------------------------
  // Live Event Stream Inspector & Polling
  // ---------------------------------------------------------------------------
  async function pollLiveEvents() {
    if (state.isStreamPaused) return;

    try {
      const topicFilter = DOM.filterTopic.value;
      const sourceFilter = DOM.filterSource.value;

      let url = `/api/events?limit=40`;
      if (state.latestSeq > 0) {
        url += `&since_seq=${state.latestSeq}`;
      }
      if (topicFilter !== "all") {
        url += `&topic=${encodeURIComponent(topicFilter)}`;
      }
      if (sourceFilter !== "all") {
        url += `&source=${encodeURIComponent(sourceFilter)}`;
      }

      const res = await apiFetch(url);
      if (res.success && res.events) {
        if (res.latest_seq > state.latestSeq) {
          state.latestSeq = res.latest_seq;
        }

        if (res.events.length > 0) {
          DOM.feedEmptyState.style.display = "none";
          res.events.forEach((item) => {
            renderEventCard(item);
          });
        }
      }
    } catch (e) {
      console.warn("Event polling error:", e);
    }
  }

  async function pollTelemetryStats() {
    try {
      const res = await apiFetch("/api/stats");
      if (res.success) {
        // State store metrics
        if (res.state_store) {
          const stats = res.state_store;
          DOM.totalEventsBadge.textContent = `${stats.total_events_emitted} Emitted`;
          DOM.bufferUsageText.textContent = `${stats.current_buffer_size}/${stats.max_buffer_size}`;

          // Update topic metric badges (4 topics)
          const topics = stats.events_by_topic || {};
          DOM.topicMetricsBar.innerHTML = `
            <span class="topic-pill users" title="ecom.users">users: ${topics["ecom.users"] || 0}</span>
            <span class="topic-pill browse" title="ecom.browse">browse: ${topics["ecom.browse"] || 0}</span>
            <span class="topic-pill cart" title="ecom.cart">cart: ${topics["ecom.cart"] || 0}</span>
            <span class="topic-pill orders" title="ecom.orders">orders: ${topics["ecom.orders"] || 0}</span>
          `;
        }

        // Simulator metrics
        if (res.simulator) {
          updateSimulatorUI(res.simulator);
        }
      }
    } catch (e) {
      console.warn("Stats poll error:", e);
    }
  }

  function renderEventCard(item) {
    const envelope = item.event;
    const topic = item.topic;
    const seq = item.seq;
    const topicClass = "topic-" + topic.replace(/\./g, "-");

    const timeFormatted = formatTime(envelope.event_timestamp);
    const payloadCompact = JSON.stringify(envelope.payload);

    const card = document.createElement("div");
    card.className = "event-card";
    card.dataset.seq = seq;

    card.innerHTML = `
      <div class="event-card-header">
        <div class="event-badge-group">
          <span class="badge-seq">#${seq}</span>
          <span class="badge-topic ${topicClass}">${topic}</span>
          <span class="badge-type">${envelope.event_type}</span>
          <span class="badge-source ${envelope.source}">${envelope.source}</span>
        </div>
        <span class="event-time">${timeFormatted}</span>
      </div>
      <div class="event-meta-line">
        <span>usr: <strong>${escapeHtml(envelope.user_id.substring(0, 14))}</strong></span>
        <span>sess: <strong>${escapeHtml(envelope.session_id.substring(0, 14))}</strong></span>
        <span>dev: <strong>${envelope.device_type}</strong></span>
      </div>
      <div class="event-payload-preview" title="Click to inspect full JSON envelope">
        ${escapeHtml(payloadCompact)}
      </div>
      <div class="event-json-viewer">
        <button type="button" class="btn-copy-json" title="Copy envelope JSON">Copy</button>
        <pre>${escapeHtml(JSON.stringify(envelope, null, 2))}</pre>
      </div>
    `;

    // Toggle full JSON inspection on preview click
    const preview = card.querySelector(".event-payload-preview");
    const viewer = card.querySelector(".event-json-viewer");
    const copyBtn = card.querySelector(".btn-copy-json");

    preview.addEventListener("click", () => {
      viewer.classList.toggle("open");
    });

    copyBtn.addEventListener("click", (e) => {
      e.stopPropagation();
      navigator.clipboard.writeText(JSON.stringify(envelope, null, 2)).then(() => {
        copyBtn.textContent = "Copied!";
        setTimeout(() => {
          copyBtn.textContent = "Copy";
        }, 1500);
      });
    });

    // Prepend to show newest events at the top
    DOM.eventFeedContainer.insertBefore(card, DOM.eventFeedContainer.firstChild);

    // Limit visible DOM elements to 100 to prevent browser slowdown
    while (DOM.eventFeedContainer.children.length > 100) {
      DOM.eventFeedContainer.removeChild(DOM.eventFeedContainer.lastChild);
    }
  }

  // ---------------------------------------------------------------------------
  // User Persona & Device Switching
  // ---------------------------------------------------------------------------
  async function handlePersonaSwitch(userId) {
    const selected = state.personas.find((p) => p.user_id === userId);
    if (!selected) return;

    try {
      const res = await apiFetch("/api/user/switch", {
        method: "POST",
        body: JSON.stringify({
          session_id: state.session.session_id,
          user_id: selected.user_id,
          device_type: state.deviceType,
          persona: selected,
        }),
      });

      if (res.success) {
        state.session = res.session;
        // If preferred category exists, filter catalog to it
        if (selected.preferred_category) {
          setCategoryTab(selected.preferred_category);
        }
      }
    } catch (e) {
      console.error("Persona switch error:", e);
    }
  }

  function handleDeviceToggle(btn) {
    DOM.deviceBtns.forEach((b) => b.classList.remove("active"));
    btn.classList.add("active");
    state.deviceType = btn.dataset.device;
  }

  // ---------------------------------------------------------------------------
  // Category & Search Filters
  // ---------------------------------------------------------------------------
  function setCategoryTab(category) {
    state.currentCategory = category;
    const tabs = DOM.categoryTabs.querySelectorAll(".tab-btn");
    tabs.forEach((tab) => {
      if (tab.dataset.category.toLowerCase() === category.toLowerCase()) {
        tab.classList.add("active");
      } else {
        tab.classList.remove("active");
      }
    });

    // Emit category_browsed event
    apiFetch("/api/events/browse", {
      method: "POST",
      body: JSON.stringify({
        session_id: state.session.session_id,
        user_id: state.session.user_id,
        device_type: state.deviceType,
        event_type: "category_browsed",
        payload: { category: category, source_trigger: "user_tab_clicked" },
      }),
    }).catch((e) => console.warn(e));

    loadCatalog();
  }

  function handleSearchInput(e) {
    clearTimeout(state.searchDebounceTimer);
    const query = e.target.value;
    state.searchQuery = query;

    state.searchDebounceTimer = setTimeout(async () => {
      await loadCatalog();

      if (query.trim().length > 1) {
        apiFetch("/api/events/browse", {
          method: "POST",
          body: JSON.stringify({
            session_id: state.session.session_id,
            user_id: state.session.user_id,
            device_type: state.deviceType,
            event_type: "product_searched",
            payload: { query: query.trim(), results_count: state.catalog.length },
          }),
        }).catch((err) => console.warn(err));
      }
    }, 400);
  }

  // ---------------------------------------------------------------------------
  // Modal Utilities
  // ---------------------------------------------------------------------------
  function openModal(modalEl) {
    if (modalEl) modalEl.classList.add("active");
  }

  function closeModal(modalEl) {
    if (modalEl) modalEl.classList.remove("active");
  }

  // ---------------------------------------------------------------------------
  // Event Listeners Setup
  // ---------------------------------------------------------------------------
  function setupEventListeners() {
    // Persona dropdown
    DOM.personaSelect.addEventListener("change", (e) => {
      handlePersonaSwitch(e.target.value);
    });

    // Device selector pills
    DOM.deviceBtns.forEach((btn) => {
      btn.addEventListener("click", () => handleDeviceToggle(btn));
    });

    // Search input & clear button
    DOM.searchInput.addEventListener("input", handleSearchInput);
    DOM.clearSearchBtn.addEventListener("click", () => {
      DOM.searchInput.value = "";
      state.searchQuery = "";
      loadCatalog();
    });

    // Category tabs
    DOM.categoryTabs.addEventListener("click", (e) => {
      const tab = e.target.closest(".tab-btn");
      if (!tab) return;
      setCategoryTab(tab.dataset.category);
    });

    // Product Grid clicks (Add to Cart vs View Detail)
    DOM.productGrid.addEventListener("click", (e) => {
      const addBtn = e.target.closest('[data-action="add-cart"]');
      if (addBtn) {
        e.stopPropagation();
        handleAddToCart(addBtn.dataset.id, 1);
        return;
      }

      const card = e.target.closest(".product-card");
      if (card) {
        openProductDetailModal(card.dataset.id);
      }
    });

    // Cart Modal Open/Close
    DOM.openCartBtn.addEventListener("click", () => openModal(DOM.cartModal));
    DOM.closeCartModalBtn.addEventListener("click", () => closeModal(DOM.cartModal));
    DOM.clearCartBtn.addEventListener("click", handleClearCart);
    DOM.proceedCheckoutBtn.addEventListener("click", openCheckoutModal);

    // Cart Items action (+, -, delete)
    DOM.cartItemsList.addEventListener("click", (e) => {
      const incBtn = e.target.closest('[data-action="inc-qty"]');
      if (incBtn) {
        handleAddToCart(incBtn.dataset.id, 1);
        return;
      }
      const decBtn = e.target.closest('[data-action="dec-qty"]');
      if (decBtn) {
        handleRemoveFromCart(decBtn.dataset.id, 1);
        return;
      }
      const delBtn = e.target.closest('[data-action="del-item"]');
      if (delBtn) {
        handleRemoveFromCart(delBtn.dataset.id, null);
        return;
      }
    });

    // Checkout Modal
    DOM.closeCheckoutModalBtn.addEventListener("click", () => closeModal(DOM.checkoutModal));
    DOM.cancelCheckoutBtn.addEventListener("click", () => {
      closeModal(DOM.checkoutModal);
      openModal(DOM.cartModal);
    });
    DOM.checkoutForm.addEventListener("submit", handleCheckoutSubmit);

    // Order Confirmation Modal
    DOM.closeConfirmModalBtn.addEventListener("click", () => closeModal(DOM.orderConfirmModal));

    // Product Modal Close
    DOM.closeProdModalBtn.addEventListener("click", () => closeModal(DOM.productModal));

    // Simulator Controls
    DOM.simToggleBtn.addEventListener("click", toggleSimulator);
    DOM.simBotsRange.addEventListener("input", updateSimulatorConfig);
    DOM.simSpeedRange.addEventListener("input", updateSimulatorConfig);

    // Event Stream Controls
    DOM.pauseStreamBtn.addEventListener("click", () => {
      state.isStreamPaused = !state.isStreamPaused;
      DOM.pauseStreamBtn.textContent = state.isStreamPaused ? "▶ Resume Feed" : "⏸ Pause Feed";
      DOM.pauseStreamBtn.classList.toggle("btn-danger", state.isStreamPaused);
    });

    DOM.clearBufferBtn.addEventListener("click", () => {
      DOM.eventFeedContainer.innerHTML = "";
      DOM.feedEmptyState.style.display = "flex";
      DOM.eventFeedContainer.appendChild(DOM.feedEmptyState);
    });

    DOM.resetStateBtn.addEventListener("click", async () => {
      if (confirm("Reset state store, clear event buffer, active sessions, and carts?")) {
        try {
          await apiFetch("/api/state/reset", { method: "POST" });
          state.latestSeq = 0;
          DOM.eventFeedContainer.innerHTML = "";
          DOM.feedEmptyState.style.display = "flex";
          DOM.eventFeedContainer.appendChild(DOM.feedEmptyState);
          await refreshCart();
          await pollTelemetryStats();
        } catch (e) {
          alert("Reset error: " + e.message);
        }
      }
    });

    // Close modals on clicking backdrop
    [DOM.productModal, DOM.cartModal, DOM.checkoutModal, DOM.orderConfirmModal].forEach(
      (backdrop) => {
        backdrop.addEventListener("click", (e) => {
          if (e.target === backdrop) closeModal(backdrop);
        });
      }
    );
  }

  // ---------------------------------------------------------------------------
  // String & Number Formatting Helpers
  // ---------------------------------------------------------------------------
  function formatTime(isoString) {
    if (!isoString) return "--:--:--";
    try {
      const d = new Date(isoString);
      return d.toTimeString().split(" ")[0] + "." + String(d.getMilliseconds()).padStart(3, "0");
    } catch {
      return isoString;
    }
  }

  function roundTo2(num) {
    return Math.round((num + Number.EPSILON) * 100) / 100;
  }

  function escapeHtml(str) {
    if (typeof str !== "string") return str;
    return str
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#039;");
  }

  // Run app on DOM load
  document.addEventListener("DOMContentLoaded", initApp);
})();
