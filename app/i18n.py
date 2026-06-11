"""
Internationalization (i18n) module for Video Server

Loads translations from locale files and provides translation functions.
Supports dynamic language switching via cookie.
"""

from pathlib import Path
from typing import Dict, Optional
from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware

from app import logger


class I18nMiddleware(BaseHTTPMiddleware):
    """Middleware to detect and set language for each request"""
    
    async def dispatch(self, request: Request, call_next):
        # Get language from cookie
        lang = request.cookies.get('lang')
        
        logger.debug(f"Cookie 'lang' value: {lang}")
        
        # If no cookie, try to detect from supported languages
        if not lang:
            # Check Accept-Language header
            accept_language = request.headers.get('Accept-Language', '')
            lang = detect_language_from_header(accept_language)
            logger.debug(f"No cookie, detected from Accept-Language: {lang}")
        
        # Validate language is supported
        if lang not in i18n.get_supported_languages():
            lang = i18n.default_language
            logger.debug(f"Language not supported, falling back to: {lang}")
        
        # Set language in request scope
        request.state.lang = lang
        
        # Log for debugging
        logger.debug(f"Request language set to: {lang}")
        
        response = await call_next(request)
        
        # Set cookie if not exists (optional)
        if not request.cookies.get('lang'):
            response.set_cookie(key='lang', value=lang, max_age=31536000, path='/')
            logger.debug(f"Set default language cookie: {lang}")
        
        return response


class I18n:
    """Internationalization manager"""
    
    def __init__(self):
        self.locales_dir = Path(__file__).parent / "locales"
        self.translations: Dict[str, Dict[str, str]] = {}
        self.supported_languages: list = []
        self.default_language = "en"
        self._load_all_translations()
    
    def _load_all_translations(self):
        """Load all translation files from locales directory"""
        if not self.locales_dir.exists():
            logger.warning(f"Locales directory not found: {self.locales_dir}")
            return
        
        for file_path in self.locales_dir.glob("*.txt"):
            lang_code = file_path.stem
            self._load_translation_file(lang_code, file_path)
        
        self.supported_languages = list(self.translations.keys())
        
        # Ensure default language exists
        if self.default_language not in self.supported_languages:
            if self.supported_languages:
                self.default_language = self.supported_languages[0]
                logger.warning(f"Default language 'en' not found, using '{self.default_language}'")
        
        if self.supported_languages:
            logger.info(f"Loaded {len(self.supported_languages)} languages: {self.supported_languages}")
        else:
            logger.warning("No translation files found, using fallback mode")
    
    def _load_translation_file(self, lang_code: str, file_path: Path):
        """Load a single translation file."""
        translations = {}
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                for line_num, line in enumerate(f, 1):
                    line = line.strip()
                    
                    if not line or line.startswith('#'):
                        continue
                    
                    if '=' not in line:
                        logger.warning(f"{lang_code}.txt line {line_num}: Missing '=', skipping")
                        continue
                    
                    key, value = line.split('=', 1)
                    key = key.strip()
                    value = value.strip()
                    
                    if not key:
                        logger.warning(f"{lang_code}.txt line {line_num}: Empty key, skipping")
                        continue
                    
                    translations[key] = value
            
            if translations:
                self.translations[lang_code] = translations
                logger.debug(f"Loaded {len(translations)} translations for '{lang_code}'")
            else:
                logger.warning(f"No valid translations found in {file_path}")
                
        except Exception as e:
            logger.error(f"Failed to load {file_path}: {e}")
    
    def translate(self, key: str, lang: Optional[str] = None, **kwargs) -> str:
        """Translate a key to the specified language."""
        if not lang or lang not in self.translations:
            lang = self.default_language
        
        translations_dict = self.translations.get(lang, {})
        text = translations_dict.get(key, key)
        
        if kwargs and '{' in text:
            try:
                text = text.format(**kwargs)
            except KeyError as e:
                logger.debug(f"Missing format key {e} for translation '{key}'")
        
        return text
    
    def get_supported_languages(self) -> list:
        """Return list of supported language codes"""
        return self.supported_languages.copy()
    
    def get_language_name(self, lang_code: str) -> str:
        """Get human-readable language name"""
        names = {
            'en': 'English',
            'ru': 'Русский',
            'uk': 'Українська',
            'be': 'Беларуская',
            'es': 'Español',
            'fr': 'Français',
            'de': 'Deutsch',
            'it': 'Italiano',
            'pt': 'Português',
            'zh': '中文',
            'ja': '日本語',
            'ko': '한국어',
        }
        return names.get(lang_code, lang_code.upper())


def detect_language_from_header(accept_language: str) -> str:
    """Parse Accept-Language header and return best matching language."""
    if not accept_language:
        return i18n.default_language
    
    # Parse quality values
    languages = []
    for part in accept_language.split(','):
        part = part.strip()
        if ';q=' in part:
            lang, q_str = part.split(';q=')
            try:
                q = float(q_str)
            except ValueError:
                q = 1.0
        else:
            lang = part
            q = 1.0
        
        # Extract base language (e.g., 'ru' from 'ru-RU')
        lang = lang.split('-')[0].lower()
        languages.append((lang, q))
    
    # Sort by quality (higher first)
    languages.sort(key=lambda x: x[1], reverse=True)
    
    # Find first supported language
    supported = i18n.get_supported_languages()
    for lang, _ in languages:
        if lang in supported:
            logger.debug(f"Detected language from header: {lang}")
            return lang
    
    return i18n.default_language


# Global instance
i18n = I18n()


def translate(key: str, lang: Optional[str] = None, **kwargs) -> str:
    """Shortcut function for translation."""
    return i18n.translate(key, lang, **kwargs)


def get_language_from_request(request: Request) -> str:
    """Get current language from request state."""
    lang = getattr(request.state, 'lang', None)
    if not lang:
        lang = i18n.default_language
        logger.debug(f"No language in request state, using default: {lang}")
    return lang


def get_language_switcher_context(request: Request) -> dict:
    """Get context for language switcher in templates."""
    current = get_language_from_request(request)
    
    languages = []
    for code in i18n.get_supported_languages():
        languages.append({
            'code': code,
            'name': i18n.get_language_name(code),
            'is_current': code == current
        })
    
    return {
        'current_lang': current,
        'languages': languages
    }