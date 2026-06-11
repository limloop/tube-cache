/**
 * Language Switcher - pure JavaScript
 * Sets cookie and reloads page to apply new language
 */

const LanguageManager = {
    cookieName: 'lang',
    
    init() {
        this.setupSwitcher();
        this.debugLog();
    },
    
    setupSwitcher() {
        const langLinks = document.querySelectorAll('.lang-switch');
        console.log('Language switcher found:', langLinks.length);
        
        langLinks.forEach(link => {
            // Remove existing listeners to prevent duplicates
            link.removeEventListener('click', this.handler);
            this.handler = (e) => {
                e.preventDefault();
                e.stopPropagation();
                const langCode = link.getAttribute('data-lang');
                if (langCode) {
                    console.log('Switching to language:', langCode);
                    this.setLanguage(langCode);
                }
            };
            link.addEventListener('click', this.handler);
        });
    },
    
    setLanguage(langCode) {
        // Set cookie with 1 year expiry, path=/
        const date = new Date();
        date.setTime(date.getTime() + (365 * 24 * 60 * 60 * 1000));
        document.cookie = `${this.cookieName}=${langCode}; expires=${date.toUTCString()}; path=/`;
        
        // Also set localStorage for debugging
        localStorage.setItem('preferred_language', langCode);
        
        console.log('Language cookie set:', document.cookie);
        
        // Reload page to apply new language
        window.location.reload();
    },
    
    getCurrentLanguage() {
        const value = `; ${document.cookie}`;
        const parts = value.split(`; ${this.cookieName}=`);
        if (parts.length === 2) {
            const lang = parts.pop().split(';').shift();
            console.log('Current language from cookie:', lang);
            return lang;
        }
        console.log('No language cookie found, using default');
        return 'en';
    },
    
    debugLog() {
        console.log('LanguageManager initialized');
        console.log('Current cookie:', document.cookie);
        console.log('Current language:', this.getCurrentLanguage());
    }
};

// Initialize when DOM ready
document.addEventListener('DOMContentLoaded', () => LanguageManager.init());