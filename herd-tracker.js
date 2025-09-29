/**
 * Herd Behavior Event Tracker
 * Lightweight JavaScript library to track user events and send them to Kafka
 */

class HerdTracker {
    constructor(config = {}) {
        this.config = {
            endpoint: config.endpoint || 'http://localhost:8000/events',
            batchSize: config.batchSize || 10,
            flushInterval: config.flushInterval || 5000, // 5 seconds
            debug: config.debug || false,
            userId: config.userId || this.generateUserId(),
            sessionId: config.sessionId || this.generateSessionId(),
            ...config
        };
        
        this.eventQueue = [];
        this.isInitialized = false;
        this.init();
    }

    init() {
        if (this.isInitialized) return;
        
        this.log('Initializing Herd Tracker...');
        
        // Start periodic flush
        setInterval(() => {
            this.flush();
        }, this.config.flushInterval);
        
        // Flush on page unload
        window.addEventListener('beforeunload', () => {
            this.flush(true);
        });
        
        // Auto-track page views
        this.trackPageView();
        
        this.isInitialized = true;
        this.log('Herd Tracker initialized');
    }

    generateUserId() {
        let userId = localStorage.getItem('herd_user_id');
        if (!userId) {
            userId = 'user_' + Math.random().toString(36).substr(2, 9);
            localStorage.setItem('herd_user_id', userId);
        }
        return userId;
    }

    generateSessionId() {
        return 'session_' + Date.now() + '_' + Math.random().toString(36).substr(2, 9);
    }

    trackEvent(eventType, data = {}) {
        const event = {
            event_type: eventType,
            user_id: this.config.userId,
            session_id: this.config.sessionId,
            timestamp: new Date().toISOString(),
            url: window.location.href,
            user_agent: navigator.userAgent,
            ...data
        };

        this.eventQueue.push(event);
        this.log('Event queued:', event);

        if (this.eventQueue.length >= this.config.batchSize) {
            this.flush();
        }
    }

    trackPageView() {
        this.trackEvent('page_view', {
            page_title: document.title,
            referrer: document.referrer
        });
    }

    trackProductView(productId, productData = {}) {
        this.trackEvent('view_product', {
            product_id: productId,
            ...productData
        });
    }

    trackAddToCart(productId, quantity = 1, productData = {}) {
        this.trackEvent('add_to_cart', {
            product_id: productId,
            quantity: quantity,
            ...productData
        });
    }

    trackPurchase(orderId, products = [], totalAmount = 0) {
        this.trackEvent('purchase', {
            order_id: orderId,
            products: products,
            total_amount: totalAmount
        });
    }

    trackSearch(query, resultsCount = 0) {
        this.trackEvent('search', {
            query: query,
            results_count: resultsCount
        });
    }

    trackCustomEvent(eventName, data = {}) {
        this.trackEvent(eventName, data);
    }

    async flush(sync = false) {
        if (this.eventQueue.length === 0) return;

        const events = [...this.eventQueue];
        this.eventQueue = [];

        this.log(`Flushing ${events.length} events...`);

        try {
            if (sync && navigator.sendBeacon) {
                // Use sendBeacon for synchronous requests (page unload)
                navigator.sendBeacon(
                    this.config.endpoint,
                    JSON.stringify({ events })
                );
            } else {
                // Use fetch for normal requests
                await fetch(this.config.endpoint, {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                    },
                    body: JSON.stringify({ events })
                });
            }
            
            this.log('Events sent successfully');
        } catch (error) {
            this.log('Error sending events:', error);
            // Re-queue events on failure
            this.eventQueue.unshift(...events);
        }
    }

    log(...args) {
        if (this.config.debug) {
            console.log('[HerdTracker]', ...args);
        }
    }

    // Auto-tracking helpers
    enableAutoTracking() {
        this.enableProductViewTracking();
        this.enableCartTracking();
        this.enableSearchTracking();
    }

    enableProductViewTracking() {
        // Track clicks on product links
        document.addEventListener('click', (event) => {
            const productLink = event.target.closest('[data-product-id]');
            if (productLink) {
                const productId = productLink.getAttribute('data-product-id');
                const productName = productLink.getAttribute('data-product-name') || '';
                const productPrice = productLink.getAttribute('data-product-price') || '';
                
                this.trackProductView(productId, {
                    product_name: productName,
                    product_price: productPrice
                });
            }
        });
    }

    enableCartTracking() {
        // Track add to cart button clicks
        document.addEventListener('click', (event) => {
            const cartButton = event.target.closest('[data-add-to-cart]');
            if (cartButton) {
                const productId = cartButton.getAttribute('data-product-id');
                const quantity = cartButton.getAttribute('data-quantity') || 1;
                const productName = cartButton.getAttribute('data-product-name') || '';
                const productPrice = cartButton.getAttribute('data-product-price') || '';
                
                this.trackAddToCart(productId, parseInt(quantity), {
                    product_name: productName,
                    product_price: productPrice
                });
            }
        });
    }

    enableSearchTracking() {
        // Track search form submissions
        document.addEventListener('submit', (event) => {
            const searchForm = event.target.closest('[data-search-form]');
            if (searchForm) {
                const searchInput = searchForm.querySelector('input[type="search"], input[name="q"], input[name="query"]');
                if (searchInput) {
                    this.trackSearch(searchInput.value);
                }
            }
        });
    }
}

// Auto-initialize if in browser environment
if (typeof window !== 'undefined') {
    window.HerdTracker = HerdTracker;
    
    // Auto-initialize with default config if data-auto-init is present
    document.addEventListener('DOMContentLoaded', () => {
        const autoInit = document.querySelector('[data-herd-auto-init]');
        if (autoInit) {
            const config = {};
            
            // Read config from data attributes
            if (autoInit.dataset.endpoint) config.endpoint = autoInit.dataset.endpoint;
            if (autoInit.dataset.debug) config.debug = autoInit.dataset.debug === 'true';
            if (autoInit.dataset.batchSize) config.batchSize = parseInt(autoInit.dataset.batchSize);
            if (autoInit.dataset.flushInterval) config.flushInterval = parseInt(autoInit.dataset.flushInterval);
            
            window.herdTracker = new HerdTracker(config);
            window.herdTracker.enableAutoTracking();
        }
    });
}

// Export for Node.js environments
if (typeof module !== 'undefined' && module.exports) {
    module.exports = HerdTracker;
}
