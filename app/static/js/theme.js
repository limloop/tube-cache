/**
 * Complete Theme Management System
 * Handles light/dark mode with localStorage, cookies, and system preference
 * Includes smooth transitions and flash prevention
 */

const ThemeManager = {
    cookieName: 'theme',
    storageKey: 'theme_preference',
    themes: ['light', 'dark'],
    transitionTimeout: null,
    
    init() {
        this.setupToggle();
        this.watchSystemPreference();
        this.addTransitionClass();
        
        // Dispatch theme loaded event
        const currentTheme = document.documentElement.getAttribute('data-theme') || 'light';
        window.dispatchEvent(new CustomEvent('themeLoaded', { detail: { theme: currentTheme } }));
    },
    
    addTransitionClass() {
        // Add transition class after initial render to prevent flash on load
        setTimeout(() => {
            document.documentElement.classList.add('theme-transition');
        }, 100);
    },
    
    applyTheme(theme, smooth = true) {
        const html = document.documentElement;
        const currentTheme = html.getAttribute('data-theme');
        
        if (currentTheme === theme) return;
        
        if (smooth) {
            // Add transition class for smooth color changes
            html.classList.add('theme-transition');
            
            // Clear existing timeout
            if (this.transitionTimeout) clearTimeout(this.transitionTimeout);
            
            // Remove transition class after animation completes
            this.transitionTimeout = setTimeout(() => {
                html.classList.remove('theme-transition');
            }, 300);
        }
        
        // Apply theme
        html.setAttribute('data-theme', theme);
        
        // Save preferences
        localStorage.setItem(this.storageKey, theme);
        this.setCookie(this.cookieName, theme, 365);
        
        // Update meta theme-color
        this.updateMetaThemeColor(theme);
        
        // Update toggle button icon
        this.updateToggleIcon(theme);
        
        // Dispatch event for other components
        window.dispatchEvent(new CustomEvent('themeChanged', { detail: { theme, smooth } }));
    },
    
    updateMetaThemeColor(theme) {
        let meta = document.querySelector('meta[name="theme-color"]');
        if (!meta) {
            meta = document.createElement('meta');
            meta.name = 'theme-color';
            document.head.appendChild(meta);
        }
        meta.content = theme === 'dark' ? '#121212' : '#ffffff';
    },
    
    toggle() {
        const current = document.documentElement.getAttribute('data-theme') || 'light';
        const next = current === 'light' ? 'dark' : 'light';
        this.applyTheme(next);
    },
    
    setupToggle() {
        const toggleBtn = document.getElementById('theme-toggle');
        if (toggleBtn) {
            toggleBtn.addEventListener('click', () => this.toggle());
        }
    },
    
    watchSystemPreference() {
        if (window.matchMedia) {
            window.matchMedia('(prefers-color-scheme: dark)').addEventListener('change', (e) => {
                // Only apply if user hasn't manually set preference
                if (!localStorage.getItem(this.storageKey)) {
                    const theme = e.matches ? 'dark' : 'light';
                    this.applyTheme(theme);
                }
            });
        }
    },
    
    updateToggleIcon(theme) {
        const toggleBtn = document.getElementById('theme-toggle');
        if (toggleBtn) {
            const icon = toggleBtn.querySelector('i');
            if (icon) {
                icon.className = theme === 'light' ? 'bi bi-moon-stars' : 'bi bi-sun';
            }
            toggleBtn.title = theme === 'light' ? 'Switch to dark mode' : 'Switch to light mode';
        }
    },
    
    getCookie(name) {
        const value = `; ${document.cookie}`;
        const parts = value.split(`; ${name}=`);
        if (parts.length === 2) return parts.pop().split(';').shift();
        return null;
    },
    
    setCookie(name, value, days) {
        const date = new Date();
        date.setTime(date.getTime() + (days * 24 * 60 * 60 * 1000));
        document.cookie = `${name}=${value}; expires=${date.toUTCString()}; path=/`;
    }
};

// Initialize after DOM is ready, but theme is already applied from inline script
document.addEventListener('DOMContentLoaded', () => ThemeManager.init());