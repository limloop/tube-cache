class TubePlayer {
    constructor(videoHash, videoTitle) {
        this.hash = videoHash;
        this.title = videoTitle;
        this.video = null;
        this.audioElement = null;
        this.isAudioMode = false;
        this.isMediaLoaded = false;
        this.speedButtons = null;
        this.controlsPanel = null;
        this.inactivityTimer = null;
        this.isControlsHidden = false;
        
        this.init();
    }
    
    init() {
        this.createControlPanel();
        this.loadProgress();
        this.loadModePreference();
        this.setupKeyboardShortcuts();
        this.setupInactivityHiding();
    }
    
    createControlPanel() {
        const container = document.querySelector('#player-container');
        if (!container) return;
        
        container.innerHTML = '';
        
        const placeholder = this.createPlaceholder();
        container.appendChild(placeholder);
        
        const panel = document.createElement('div');
        panel.className = 'player-controls p-2 bg-dark text-white d-flex justify-content-between align-items-center flex-wrap';
        panel.style.gap = '10px';
        panel.style.transition = 'opacity 0.3s ease';
        
        panel.innerHTML = `
            <div class="btn-group btn-group-sm mb-2 mb-sm-0">
                <button class="btn btn-outline-light" data-speed="0.5">0.5x</button>
                <button class="btn btn-outline-light" data-speed="0.75">0.75x</button>
                <button class="btn btn-outline-light active" data-speed="1">1x</button>
                <button class="btn btn-outline-light" data-speed="1.25">1.25x</button>
                <button class="btn btn-outline-light" data-speed="1.5">1.5x</button>
                <button class="btn btn-outline-light" data-speed="2">2x</button>
            </div>
            <div class="btn-group btn-group-sm">
                <button class="btn btn-outline-light" id="play-video-btn">
                    <i class="bi bi-camera-video"></i> Видео
                </button>
                <button class="btn btn-outline-light" id="play-audio-btn">
                    <i class="bi bi-music-note"></i> Аудио
                </button>
            </div>
        `;
        
        container.parentElement.insertBefore(panel, container);
        this.controlsPanel = panel;
        
        this.speedButtons = panel.querySelectorAll('[data-speed]');
        
        document.getElementById('play-video-btn').addEventListener('click', () => this.initVideo());
        document.getElementById('play-audio-btn').addEventListener('click', () => this.initAudio());
    }
    
    setupInactivityHiding() {
        const container = document.querySelector('#player-container');
        if (!container) return;
        
        const resetTimer = () => {
            if (this.inactivityTimer) clearTimeout(this.inactivityTimer);
            this.showControls();
            this.inactivityTimer = setTimeout(() => this.hideControls(), 3000);
        };
        
        container.addEventListener('mousemove', resetTimer);
        container.addEventListener('click', resetTimer);
        container.addEventListener('mouseleave', () => {
            if (this.inactivityTimer) clearTimeout(this.inactivityTimer);
            this.showControls();
        });
        
        if (this.video) {
            this.video.addEventListener('mouseenter', resetTimer);
        }
    }
    
    hideControls() {
        if (this.controlsPanel && this.isMediaLoaded && !this.isAudioMode) {
            this.controlsPanel.style.opacity = '0';
            this.controlsPanel.style.pointerEvents = 'none';
            this.isControlsHidden = true;
        }
    }
    
    showControls() {
        if (this.controlsPanel) {
            this.controlsPanel.style.opacity = '1';
            this.controlsPanel.style.pointerEvents = 'auto';
            this.isControlsHidden = false;
        }
    }
    
    createPlaceholder() {
        const div = document.createElement('div');
        div.className = 'placeholder-player d-flex flex-column align-items-center justify-content-center';
        div.style.minHeight = '400px';
        
        div.innerHTML = `
            <i class="bi bi-play-circle" style="font-size: 4rem;"></i>
            <h5 class="mt-3">Выберите режим воспроизведения</h5>
            <div class="mt-2">
                <button class="btn btn-light m-1" onclick="window.player.initVideo()">
                    <i class="bi bi-camera-video"></i> Видео
                </button>
                <button class="btn btn-light m-1" onclick="window.player.initAudio()">
                    <i class="bi bi-music-note"></i> Аудио
                </button>
            </div>
        `;
        
        div.addEventListener('click', (e) => {
            if (e.target.tagName !== 'BUTTON') this.initVideo();
        });
        
        return div;
    }
    
    initVideo() {
        if (this.video) {
            this.showVideoMode();
            return;
        }
        
        this.video = document.createElement('video');
        this.video.controls = true;
        this.video.className = 'w-100';
        this.video.style.display = 'block';
        this.video.preload = 'metadata';
        this.video.src = `/stream/${this.hash}`;
        this.video.load();
        
        const container = document.querySelector('#player-container');
        
        // Сохраняем аудио если есть
        if (this.audioElement) {
            // Не затираем контейнер, просто добавляем видео
            container.appendChild(this.video);
            this.audioElement.style.display = 'none';
            this.video.style.display = 'block';
        } else {
            container.innerHTML = '';
            container.appendChild(this.video);
        }
        
        const savedTime = localStorage.getItem(`video_${this.hash}_time`);
        if (savedTime) this.video.currentTime = parseFloat(savedTime);
        
        this.attachVideoEvents();
        this.setupSpeedControls();
        this.setupInactivityHiding();
        
        this.isMediaLoaded = true;
        this.isAudioMode = false;
        localStorage.setItem(`player_mode_${this.hash}`, 'video');
        
        this.video.play().catch(e => console.log('Autoplay blocked:', e));
    }

    initAudio() {
        if (this.audioElement) {
            this.showAudioMode();
            return;
        }
        
        this.audioElement = document.createElement('audio');
        this.audioElement.controls = true;
        this.audioElement.className = 'w-100';
        this.audioElement.style.display = 'block';
        this.audioElement.preload = 'metadata';
        this.audioElement.src = `/stream/${this.hash}`;
        this.audioElement.load();
        
        const container = document.querySelector('#player-container');
        
        // Сохраняем видео если есть
        if (this.video) {
            container.appendChild(this.audioElement);
            this.video.style.display = 'none';
            this.audioElement.style.display = 'block';
        } else {
            container.innerHTML = '';
            container.appendChild(this.audioElement);
        }
        
        const savedTime = localStorage.getItem(`video_${this.hash}_time`);
        if (savedTime) this.audioElement.currentTime = parseFloat(savedTime);
        
        this.attachAudioEvents();
        
        this.isMediaLoaded = true;
        this.isAudioMode = true;
        localStorage.setItem(`player_mode_${this.hash}`, 'audio');
        
        this.audioElement.play().catch(e => console.log('Autoplay blocked:', e));
    }

    showVideoMode() {
        if (!this.video) {
            // Если видео нет, создаём его
            this.initVideo();
            return;
        }
        
        // Останавливаем аудио
        if (this.audioElement && !this.audioElement.paused) {
            this.audioElement.pause();
        }
        
        // Синхронизируем время
        if (this.audioElement) {
            this.video.currentTime = this.audioElement.currentTime;
        }
        
        // Переключаем видимость
        this.video.style.display = 'block';
        if (this.audioElement) this.audioElement.style.display = 'none';
        
        this.isAudioMode = false;
        localStorage.setItem(`player_mode_${this.hash}`, 'video');
        
        // Запускаем видео
        this.video.play().catch(e => console.log('Playback error:', e));
    }

    showAudioMode() {
        if (!this.audioElement) {
            // Если аудио нет, создаём его
            this.initAudio();
            return;
        }
        
        // Останавливаем видео
        if (this.video && !this.video.paused) {
            this.video.pause();
        }
        
        // Синхронизируем время
        if (this.video) {
            this.audioElement.currentTime = this.video.currentTime;
        }
        
        // Переключаем видимость
        this.audioElement.style.display = 'block';
        if (this.video) this.video.style.display = 'none';
        
        this.isAudioMode = true;
        localStorage.setItem(`player_mode_${this.hash}`, 'audio');
        
        if (this.controlsPanel) {
            this.controlsPanel.style.opacity = '1';
            this.controlsPanel.style.pointerEvents = 'auto';
        }
        
        // Запускаем аудио
        this.audioElement.play().catch(e => console.log('Playback error:', e));
    }
    
    attachVideoEvents() {
        let lastSave = 0;
        
        this.video.addEventListener('timeupdate', () => {
            const now = Math.floor(this.video.currentTime);
            if (now % 5 === 0 && now !== lastSave) {
                localStorage.setItem(`video_${this.hash}_time`, this.video.currentTime);
                lastSave = now;
            }
        });
        
        this.video.addEventListener('ratechange', () => {
            localStorage.setItem(`playback_rate_${this.hash}`, this.video.playbackRate);
        });
        
        this.video.addEventListener('ended', () => {
            localStorage.removeItem(`video_${this.hash}_time`);
        });
    }
    
    attachAudioEvents() {
        let lastSave = 0;
        
        this.audioElement.addEventListener('timeupdate', () => {
            const now = Math.floor(this.audioElement.currentTime);
            if (now % 5 === 0 && now !== lastSave) {
                localStorage.setItem(`video_${this.hash}_time`, this.audioElement.currentTime);
                lastSave = now;
            }
        });
        
        this.audioElement.addEventListener('ended', () => {
            localStorage.removeItem(`video_${this.hash}_time`);
        });
    }
    
    setupSpeedControls() {
        this.speedButtons.forEach(btn => {
            btn.addEventListener('click', () => {
                const speed = parseFloat(btn.dataset.speed);
                const media = this.isAudioMode ? this.audioElement : this.video;
                
                if (media) {
                    media.playbackRate = speed;
                    this.speedButtons.forEach(b => b.classList.remove('active'));
                    btn.classList.add('active');
                    localStorage.setItem(`playback_rate_${this.hash}`, speed);
                }
            });
        });
        
        const savedRate = localStorage.getItem(`playback_rate_${this.hash}`);
        if (savedRate) {
            const btn = Array.from(this.speedButtons).find(b => parseFloat(b.dataset.speed) === parseFloat(savedRate));
            if (btn) btn.click();
        }
    }
    
    loadProgress() {}
    
    loadModePreference() {
        const savedMode = localStorage.getItem(`player_mode_${this.hash}`);
        if (savedMode) {
            setTimeout(() => {
                this.showNotification(
                    `Продолжить в ${savedMode === 'video' ? 'видео' : 'аудио'}режиме?`,
                    true
                );
            }, 500);
        }
    }
    
    setupKeyboardShortcuts() {
        document.addEventListener('keydown', (e) => {
            if (e.target.tagName === 'INPUT' || e.target.tagName === 'TEXTAREA') return;
            if (!this.isMediaLoaded) return;
            
            const media = this.isAudioMode ? this.audioElement : this.video;
            if (!media) return;
            
            switch(e.key) {
                case 'ArrowLeft':
                    media.currentTime -= 5;
                    e.preventDefault();
                    break;
                case 'ArrowRight':
                    media.currentTime += 5;
                    e.preventDefault();
                    break;
                case ' ':
                case 'Space':
                    if (media.paused) media.play();
                    else media.pause();
                    e.preventDefault();
                    break;
                case 'm':
                    media.muted = !media.muted;
                    break;
                case 'v':
                    if (this.video && this.audioElement) {
                        if (this.isAudioMode) this.showVideoMode();
                        else this.showAudioMode();
                    }
                    break;
                case 'f':
                    if (this.video && !this.isAudioMode) {
                        if (this.video.requestFullscreen) {
                            this.video.requestFullscreen();
                        }
                    }
                    break;
            }
        });
    }
    
    showNotification(message, withButtons = false) {
        const toast = document.createElement('div');
        toast.className = 'alert alert-info toast-notification';
        
        if (withButtons && message.includes('Продолжить')) {
            toast.innerHTML = `
                ${message}
                <div class="mt-2">
                    <button class="btn btn-sm btn-success me-1" onclick="window.player.${this.isAudioMode ? 'showAudioMode' : 'showVideoMode'}()">Да</button>
                    <button class="btn btn-sm btn-secondary" onclick="this.closest('.alert').remove()">Нет</button>
                </div>
            `;
        } else {
            toast.innerHTML = message;
            setTimeout(() => toast.remove(), 2000);
        }
        
        document.body.appendChild(toast);
    }
}