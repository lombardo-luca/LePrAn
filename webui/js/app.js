// ============================================
// LePrAn - Letterboxd Profile Analyzer
// Alpine.js Application Logic
// ============================================

function lepranApp() {
    return {
         // State
         logoPath: 'assets/logo.png',
         selectedFile: null,
         selectedFileName: '',
         isAnalyzing: false,
         progressPercent: 0,
         analysisProgress: 'Starting...',
         hasResults: false,
         
         // Detailed progress stats
         filmsProcessed: 0,
         filmsTotal: 0,
         processingSpeed: 0,
         etaSeconds: 0,
        
        // Reference to this app instance for global callback
        _appRef: null,
        
        // Charts instances
        charts: {},
        
        // Stats data
        stats: {
            username: '',
            filmsCount: 0,
            totalRuntime: '0h',
            totalHours: 0,
            scrapedAt: ''
        },
        
        // Raw category data from backend
        rawData: {
            countries: [],
            languages: [],
            genres: [],
            directors: [],
            actors: []
        },
        
        // Computed sorted data (for display)
        get sortedData() {
            const result = {};
            for (const key of Object.keys(this.rawData)) {
                result[key] = [...this.rawData[key]].sort((a, b) => b.count - a.count);
            }
            return result;
        },
        
        // Initialize
        init() {
            console.log('LePrAn Web UI initialized');
            
            // Store reference for global callback
            this._appRef = this;
            
            // Check if we're running in pywebview
            if (window.pywebview) {
                console.log('pywebview API detected');
            } else {
                console.warn('Running without pywebview bridge - using demo mode');
            }
        },
        
        // Handle file selection from input
        handleFileSelect(event) {
            const file = event.target.files[0];
            if (file) {
                this.selectedFile = file;
                this.selectedFileName = file.name;
                console.log('Selected file:', file.name);
            }
        },
        
        // Analyze selected CSV with TMDB
        async analyzeCSV() {
            if (!this.selectedFile) {
                alert('Please select a CSV file first.');
                return;
            }
            
            this.isAnalyzing = true;
            this.progressPercent = 0;
            this.analysisProgress = 'Starting analysis...';
            this.hasResults = false;
            
            try {
                // Read the file as text and send to Python backend
                const reader = new FileReader();
                
                reader.onload = (e) => {
                    const csvContent = e.target.result;
                    
                    if (window.pywebview && window.pywebview.api) {
                        // Start analysis (returns immediately)
                        window.pywebview.api.analyze_csv_content(csvContent).then((result) => {
                            if (result.status === 'started') {
                                // Start polling for progress
                                this._startProgressPolling();
                            } else if (result.success) {
                                // Count-only result returned directly
                                this.onAnalysisComplete(result);
                            }
                        }).catch((err) => {
                            this.onAnalysisError(err);
                        });
                    } else {
                        // Demo mode - simulate analysis
                        this.simulateAnalysis();
                    }
                };
                
                reader.onerror = () => {
                    this.onAnalysisError('Failed to read file');
                };
                
                reader.readAsText(this.selectedFile);
                
            } catch (error) {
                this.onAnalysisError(error.message || 'Unknown error');
            }
        },
        
        // Start polling for analysis progress
        _startProgressPolling() {
            // Clear any existing polling interval
            if (this._progressInterval) {
                clearInterval(this._progressInterval);
            }
            
            // Poll every 500ms
            this._progressInterval = setInterval(async () => {
                try {
                    const progress = await window.pywebview.api.get_analysis_progress();
                    
                    // Update progress bar and status
                    this.progressPercent = progress.percent;
                    this.analysisProgress = progress.status;
                    
                    // Update detailed progress stats
                    this.filmsProcessed = progress.films_processed || 0;
                    this.filmsTotal = progress.films_total || 0;
                    this.processingSpeed = progress.speed || 0;
                    this.etaSeconds = progress.eta_seconds || 0;
                    
                    // Check if analysis is complete
                    if (!progress.running) {
                        clearInterval(this._progressInterval);
                        this._progressInterval = null;
                        
                        if (progress.error) {
                            this.onAnalysisError(progress.error);
                        } else if (progress.result) {
                            this.onAnalysisComplete(progress.result);
                        }
                    }
                } catch (err) {
                    console.error('Progress poll error:', err);
                }
            }, 500);
        },
        
        // Load saved LePrAn CSV data
        async loadSavedCSV() {
            this.isAnalyzing = true;
            this.progressPercent = 0;
            this.analysisProgress = 'Loading saved data...';
            
            try {
                if (window.pywebview && window.pywebview.api) {
                    // Open file dialog in Python and load the CSV
                    const result = await window.pywebview.api.load_saved_csv();
                    this.onAnalysisComplete(result);
                } else {
                    // Demo mode - use a sample file picker
                    this.simulateLoadCSV();
                }
            } catch (error) {
                this.onAnalysisError(error.message || 'Failed to load CSV');
            }
        },
        
        // Handle successful analysis completion
        onAnalysisComplete(result) {
            this.isAnalyzing = false;
            this.progressPercent = 100;
            this.analysisProgress = 'Complete!';
            this.hasResults = true;
            
            // Reset detailed progress stats
            this.filmsProcessed = 0;
            this.filmsTotal = 0;
            this.processingSpeed = 0;
            this.etaSeconds = 0;
            
            // Clean up polling interval if still active
            if (this._progressInterval) {
                clearInterval(this._progressInterval);
                this._progressInterval = null;
            }
            
            // Parse result data
            if (result.success) {
                this.stats = {
                    username: result.username || 'Unknown',
                    filmsCount: result.films_count || 0,
                    totalRuntime: this.formatRuntime(result.total_hours || 0),
                    totalHours: result.total_hours || 0,
                    scrapedAt: result.scraped_at || new Date().toLocaleDateString()
                };
                
                // Store category data
                this.rawData.countries = this.parseDictResult(result.countries);
                this.rawData.languages = this.parseDictResult(result.languages);
                this.rawData.genres = this.parseDictResult(result.genres);
                this.rawData.directors = this.parseDictResult(result.directors);
                this.rawData.actors = this.parseDictResult(result.actors);
                
                // Initialize charts after DOM updates
                this.$nextTick(() => {
                    this.initCharts();
                });
            } else {
                alert('Analysis failed: ' + (result.error || 'Unknown error'));
                this.hasResults = false;
            }
        },
        
        // Handle analysis error
        onAnalysisError(error) {
            this.isAnalyzing = false;
            this.progressPercent = 0;
            this.analysisProgress = 'Failed';
            
            // Reset detailed progress stats
            this.filmsProcessed = 0;
            this.filmsTotal = 0;
            this.processingSpeed = 0;
            this.etaSeconds = 0;
            
            // Clean up polling interval if still active
            if (this._progressInterval) {
                clearInterval(this._progressInterval);
                this._progressInterval = null;
            }
            
            alert('Error: ' + (error || 'Analysis failed'));
        },
        
        // Parse dictionary results from Python into array format
        parseDictResult(dictData) {
            if (!dictData) return [];
            
            try {
                // Handle both string and object formats
                let parsed;
                if (typeof dictData === 'string') {
                    parsed = JSON.parse(dictData);
                } else {
                    parsed = dictData;
                }
                
                return Object.entries(parsed).map(([name, count]) => ({
                    name: String(name),
                    count: Number(count),
                    percent: this.calculatePercent(Number(count))
                }));
            } catch (e) {
                console.error('Failed to parse dict data:', e);
                return [];
            }
        },
        
        // Calculate percentage for a count value
        calculatePercent(count) {
            if (!this.stats.filmsCount || this.stats.filmsCount === 0) return '0.00%';
            const percent = (count / this.stats.filmsCount) * 100;
            return percent.toFixed(2) + '%';
        },
        
        // Format total hours into readable string
        formatRuntime(hours) {
            if (!hours || hours === 0) return '0h';
            const h = Math.floor(hours);
            const m = Math.round((hours - h) * 60);
            if (m === 0) return `${h}h`;
            return `${h}h ${m}m`;
        },
        
        // Format ETA in seconds to readable string (e.g., "2m30s", "1h15m", "45s")
        formatETA(seconds) {
            if (!seconds || seconds <= 0) return 'Calculating...';
            const s = Math.round(seconds);
            if (s < 60) return `${s}s`;
            const m = Math.floor(s / 60);
            const remS = s % 60;
            if (m < 60) return remS > 0 ? `${m}m${remS}s` : `${m}m`;
            const h = Math.floor(m / 60);
            const remM = m % 60;
            return remM > 0 ? `${h}h${remM}m` : `${h}h`;
        },
        
        // Reset to input screen for new analysis
        resetToInput() {
            // Clean up polling interval if still active
            if (this._progressInterval) {
                clearInterval(this._progressInterval);
                this._progressInterval = null;
            }
            
            this.hasResults = false;
            this.selectedFile = null;
            this.selectedFileName = '';
            this.stats = { username: '', filmsCount: 0, totalRuntime: '0h', totalHours: 0, scrapedAt: '' };
            this.rawData = { countries: [], languages: [], genres: [], directors: [], actors: [] };
            
            // Destroy charts to free memory
            this.destroyCharts();
        },
        
        // Save results to CSV
        async saveResults() {
            if (!this.hasResults) return;
            
            try {
                if (window.pywebview && window.pywebview.api) {
                    const result = await window.pywebview.api.save_results({
                        username: this.stats.username,
                        films_count: this.stats.filmsCount,
                        total_hours: this.stats.totalHours,
                        scraped_at: this.stats.scrapedAt,
                        countries: this.rawData.countries,
                        languages: this.rawData.languages,
                        genres: this.rawData.genres,
                        directors: this.rawData.directors,
                        actors: this.rawData.actors
                    });
                    
                    if (result && result.success) {
                        alert('Results saved successfully!');
                    } else {
                        alert('Failed to save results: ' + (result?.error || 'Unknown error'));
                    }
                } else {
                    // Demo mode - download as JSON
                    this.downloadDemoSave();
                }
            } catch (error) {
                alert('Error saving results: ' + (error.message || 'Unknown error'));
            }
        },
        
        // Initialize Chart.js charts for all panels
        initCharts() {
            this.destroyCharts();
            
            const chartConfigs = [
                { id: 'countriesChart', data: this.rawData.countries, color: '#58a6ff' },
                { id: 'languagesChart', data: this.rawData.languages, color: '#3fb950' },
                { id: 'genresChart', data: this.rawData.genres, color: '#bc8cff' },
                { id: 'directorsChart', data: this.rawData.directors, color: '#d29922' },
                { id: 'actorsChart', data: this.rawData.actors, color: '#f85149' }
            ];
            
            chartConfigs.forEach(config => {
                const canvas = document.getElementById(config.id);
                if (!canvas || !config.data.length) return;
                
                // Sort by count descending and get top 10 items for chart readability
                const topItems = [...config.data].sort((a, b) => b.count - a.count).slice(0, 10);
                
                this.charts[config.id] = new Chart(canvas, {
                    type: 'bar',
                    data: {
                        labels: topItems.map(item => item.name),
                        datasets: [{
                            label: 'Films',
                            data: topItems.map(item => item.count),
                            backgroundColor: config.color + '99',
                            borderColor: config.color,
                            borderWidth: 1,
                            borderRadius: 4
                        }]
                    },
                    options: {
                        responsive: true,
                        maintainAspectRatio: false,
                        indexAxis: 'y',
                        plugins: {
                            legend: {
                                display: false
                            },
                            tooltip: {
                                backgroundColor: '#21262d',
                                titleColor: '#e6edf3',
                                bodyColor: '#8b949e',
                                borderColor: '#30363d',
                                borderWidth: 1,
                                padding: 10,
                                displayColors: false
                            }
                        },
                        scales: {
                            x: {
                                beginAtZero: true,
                                grid: {
                                    color: 'rgba(48, 54, 61, 0.5)'
                                },
                                ticks: {
                                    color: '#8b949e',
                                    stepSize: 1
                                }
                            },
                            y: {
                                grid: {
                                    display: false
                                },
                                ticks: {
                                    color: '#8b949e',
                                    font: { size: 11 }
                                }
                            }
                        }
                    }
                });
            });
        },
        
        // Destroy all chart instances
        destroyCharts() {
            for (const id in this.charts) {
                if (this.charts[id]) {
                    this.charts[id].destroy();
                    this.charts[id] = null;
                }
            }
        },
        
        // Demo mode: simulate analysis with sample data
        simulateAnalysis() {
            const delay = (ms) => new Promise(resolve => setTimeout(resolve, ms));
            
            // Simulate progress updates
            const progressInterval = setInterval(() => {
                if (this.progressPercent < 90) {
                    this.progressPercent += Math.random() * 15;
                    if (this.progressPercent > 90) this.progressPercent = 90;
                    this.analysisProgress = Math.round(this.progressPercent) + '%';
                }
            }, 500);
            
            setTimeout(async () => {
                clearInterval(progressInterval);
                this.progressPercent = 100;
                this.analysisProgress = '100%';
                
                // Generate sample data
                const sampleData = this.generateSampleData();
                this.onAnalysisComplete(sampleData);
            }, 3000);
        },
        
        // Demo mode: simulate loading saved CSV
        simulateLoadCSV() {
            setTimeout(() => {
                this.progressPercent = 100;
                this.analysisProgress = '100%';
                
                const sampleData = this.generateSampleData();
                this.onAnalysisComplete(sampleData);
            }, 1500);
        },
        
        // Generate sample data for demo mode
        generateSampleData() {
            return {
                success: true,
                username: 'demo_user',
                films_count: 142,
                total_hours: 284.5,
                scraped_at: new Date().toLocaleDateString(),
                countries: JSON.stringify({
                    'United States': 67,
                    'United Kingdom': 23,
                    'France': 15,
                    'Japan': 12,
                    'Canada': 8,
                    'Germany': 6,
                    'Australia': 5,
                    'India': 3,
                    'South Korea': 2,
                    'Italy': 1
                }),
                languages: JSON.stringify({
                    'English': 98,
                    'French': 15,
                    'Japanese': 12,
                    'German': 6,
                    'Spanish': 4,
                    'Hindi': 3,
                    'Korean': 2,
                    'Italian': 1,
                    'Mandarin': 1
                }),
                genres: JSON.stringify({
                    'Drama': 52,
                    'Comedy': 28,
                    'Action': 22,
                    'Thriller': 18,
                    'Sci-Fi': 12,
                    'Romance': 6,
                    'Horror': 4
                }),
                directors: JSON.stringify({
                    'Christopher Nolan': 8,
                    'Quentin Tarantino': 7,
                    'Denis Villeneuve': 5,
                    'Jordan Peele': 4,
                    'Greta Gerwig': 4,
                    'Bong Joon-ho': 3,
                    'Martin Scorsese': 3,
                    'Steven Spielberg': 2
                }),
                actors: JSON.stringify({
                    'Leonardo DiCaprio': 12,
                    'Brad Pitt': 10,
                    'Margot Robbie': 8,
                    'Tom Hanks': 7,
                    'Scarlett Johansson': 6,
                    'Ryan Gosling': 5,
                    'Florence Pugh': 4,
                    'Idris Elba': 4,
                    'Saoirse Ronan': 3,
                    'Timothée Chalamet': 3
                })
            };
        },
        
        // Demo mode: download saved data as JSON file
        downloadDemoSave() {
            const data = {
                username: this.stats.username,
                films_count: this.stats.filmsCount,
                scraped_at: this.stats.scrapedAt,
                countries: this.rawData.countries,
                languages: this.rawData.languages,
                genres: this.rawData.genres,
                directors: this.rawData.directors,
                actors: this.rawData.actors
            };
            
            const blob = new Blob([JSON.stringify(data, null, 2)], { type: 'application/json' });
            const url = URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            a.download = `${this.stats.username}_lepran_results.json`;
            a.click();
            URL.revokeObjectURL(url);
        }
    };
}

// ============================================
// Global progress callback (called from Python via evaluate_js)
// This receives real-time progress updates pushed from the backend.
// Signature: __lepranProgress(percent, status, filmsProcessed, filmsTotal, speed, etaSeconds)
// ============================================
window.__lepranProgress = function(percent, status, filmsProcessed, filmsTotal, speed, etaSeconds) {
    // Try to find the Alpine.js component instance
    const appEl = document.querySelector('[x-data="lepranApp"]');
    if (appEl && appEl.__x) {
        const data = appEl.__x.$data;
        data.progressPercent = percent;
        data.analysisProgress = status;
        data.filmsProcessed = filmsProcessed || 0;
        data.filmsTotal = filmsTotal || 0;
        data.processingSpeed = speed || 0;
        data.etaSeconds = etaSeconds || 0;
    }
};
