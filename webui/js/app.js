// ============================================
// LePrAn - Letterboxd Profile Analyzer
// Alpine.js Application Logic
// ============================================

function lepranApp() {
    return {
         // State
          logoPath: 'assets/logo.png',
          selectedFolder: null,
          selectedFolderName: '',
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
             totalDays: 0,
             scrapedAt: ''
         },
        
         // Panel state (moved to parent so both columns share the same scope)
         leftPanel: 'countries',
         rightPanel: 'actors',
         
         // Display limit for all categories
         DISPLAY_LIMIT: 100,
         
         // Category expansion states (each category can be expanded to show all entries)
          countriesExpanded: false,
          languagesExpanded: false,
          genresExpanded: false,
          directorsExpanded: false,
          actorsExpanded: false,
          decadesExpanded: false,
          diaryExpanded: false,
          financialExpanded: false,
         
          // Raw category data from backend
          rawData: {
              countries: [],
              languages: [],
              genres: [],
              directors: [],
              actors: [],
              decades: []
          },
          
           // Raw diary/financial data from backend
           rawDiaryData: {
               weekday: [],
               month: [],
               year: []
           },
           
           // Computed: films watched in the current system year
           get filmsThisYear() {
               const currentYear = new Date().getFullYear().toString();
               const yearData = this.rawDiaryData.year || [];
               const entry = yearData.find(item => item.name === currentYear);
               return entry ? entry.count : 0;
           },
          rawFinancialData: {
              budget: [],
              boxoffice: []
          },
          
           // Chart type preferences per category ('bar' or 'pie')
           // Diary has independent chart types per aggregation mode (matching Film panel pattern)
           chartTypes: {
               countries: 'bar',
               languages: 'bar',
               genres: 'bar',
               directors: 'bar',
               actors: 'bar',
               decades: 'bar',
               diaryWeekday: 'bar',
               diaryMonth: 'bar',
               diaryYear: 'bar',
               financial: 'bar'
           },
          
          // Diary aggregation mode: 'weekday', 'month', or 'year'
          diaryAggregationMode: 'weekday',
          
          // Financial view mode: 'budget' or 'boxoffice'
          financialViewMode: 'budget',
          
          // Pie chart slice limit (top N slices, rest grouped as "Other")
          pieSliceLimit: 7,
          
          // Financial value formatting helper
          formatFinancialValue(value, mode) {
              const num = parseFloat(value);
              if (isNaN(num)) return '$0';
              const formatted = num.toLocaleString('en-US');
              return '$' + formatted;
          },
         
          // Computed sorted data (for display)
          // Each category is limited to DISPLAY_LIMIT entries unless its respective expanded state is true
          get sortedData() {
              const result = {};
              const expansionStates = {
                  countries: this.countriesExpanded,
                  languages: this.languagesExpanded,
                  genres: this.genresExpanded,
                  directors: this.directorsExpanded,
                  actors: this.actorsExpanded,
                  decades: this.decadesExpanded
              };
              for (const key of Object.keys(this.rawData)) {
                  let sorted = [...this.rawData[key]].sort((a, b) => b.count - a.count);
                  
                  // Limit to top 100 unless that category is expanded
                  if (!expansionStates[key]) {
                      sorted = sorted.slice(0, this.DISPLAY_LIMIT);
                  }
                  
                  result[key] = sorted;
              }
              return result;
          },
          
          // Computed: sorted diary data based on current aggregation mode
          // Limited to DISPLAY_LIMIT entries unless diaryExpanded is true
          get sortedDiaryData() {
              let data = this.rawDiaryData[this.diaryAggregationMode] || [];
              data = [...data].sort((a, b) => b.count - a.count);
              if (!this.diaryExpanded) {
                  data = data.slice(0, this.DISPLAY_LIMIT);
              }
              return data;
          },
          
          // Computed: sorted financial data based on current view mode
          // Limited to DISPLAY_LIMIT entries unless financialExpanded is true
          get sortedFinancialData() {
              let data = this.rawFinancialData[this.financialViewMode] || [];
              data = [...data].sort((a, b) => b.count - a.count);
              if (!this.financialExpanded) {
                  data = data.slice(0, this.DISPLAY_LIMIT);
              }
              return data;
          },
        
        // Initialize
        init() {
            console.log('LePrAn Web UI initialized');
            
            // Store reference for global callback
            this._appRef = this;
            
            // Load saved chart type preferences from localStorage
            this.loadChartTypePreferences();
            
            // Check if we're running in pywebview
            if (window.pywebview) {
                console.log('pywebview API detected');
            } else {
                console.warn('Running without pywebview bridge - using demo mode');
            }
        },
        
        // Load chart type preferences from localStorage
        loadChartTypePreferences() {
            try {
                const saved = localStorage.getItem('lepran_chartTypes');
                if (saved) {
                    const parsed = JSON.parse(saved);
                    // Merge with defaults
                    this.chartTypes = { ...this.chartTypes, ...parsed };
                }
            } catch (e) {
                console.warn('Failed to load chart type preferences:', e);
            }
        },
        
        // Save chart type preferences to localStorage
        saveChartTypePreferences() {
            try {
                localStorage.setItem('lepran_chartTypes', JSON.stringify(this.chartTypes));
            } catch (e) {
                console.warn('Failed to save chart type preferences:', e);
            }
        },
        
        // Toggle chart type for a given category (only recreates that specific chart)
        toggleChartType(category) {
            if (this.chartTypes[category] === 'bar') {
                this.chartTypes[category] = 'pie';
            } else {
                this.chartTypes[category] = 'bar';
            }
            this.saveChartTypePreferences();
            // Only recreate the specific chart for this category
            this.$nextTick(() => {
                this.recreateChartForCategory(category);
            });
        },
        
        // Recreate only the chart for a specific category
        recreateChartForCategory(category) {
            const categoryToChartId = {
                countries: 'countriesChart',
                languages: 'languagesChart',
                genres: 'genresChart',
                directors: 'directorsChart',
                actors: 'actorsChart',
                decades: 'decadesChart',
                diaryWeekday: 'diaryChart',
                diaryMonth: 'diaryChart',
                diaryYear: 'diaryChart',
                financial: 'financialChart'
            };
            const chartId = categoryToChartId[category];
            if (!chartId) return;
            
            const canvas = document.getElementById(chartId);
            if (!canvas) return;
            
            // Destroy only this specific chart using Chart.js registry
            const existingChart = Chart.getChart(canvas);
            if (existingChart) {
                existingChart.destroy();
            }
            
            // Clear our internal reference (don't call destroy again - already done above)
            delete this.charts[chartId];
            
            // Small delay to ensure Chart.js fully releases the canvas
            setTimeout(() => {
                this._doCreateChart(category, chartId, canvas);
            }, 50);
        },
        
        // Internal method to actually create the chart (called after delay)
        _doCreateChart(category, chartId, canvas) {
            // Double-check: destroy any chart that might have been created in the meantime
            const stillExisting = Chart.getChart(canvas);
            if (stillExisting) {
                stillExisting.destroy();
            }
            delete this.charts[chartId];
            
            // Data and color mappings
            const dataMap = {
                countries: this.rawData.countries,
                languages: this.rawData.languages,
                genres: this.rawData.genres,
                directors: this.rawData.directors,
                actors: this.rawData.actors,
                decades: this.rawData.decades,
                diaryWeekday: this.sortedDiaryData,
                diaryMonth: this.sortedDiaryData,
                diaryYear: this.sortedDiaryData,
                financial: this.sortedFinancialData
            };
            const colorMap = {
                countries: '#58a6ff',
                languages: '#3fb950',
                genres: '#bc8cff',
                directors: '#d29922',
                actors: '#f85149',
                decades: '#f0883e',
                diaryWeekday: '#58a6ff',
                diaryMonth: '#58a6ff',
                diaryYear: '#58a6ff',
                financial: '#3fb950'
            };
            
            this._createChart({
                id: chartId,
                data: dataMap[category],
                color: colorMap[category],
                category: category
            });
        },
        
        // Get display label for current chart type (shows what to switch TO)
        getChartTypeLabel(category) {
            const currentType = this.chartTypes[category] || 'bar';
            const targetType = currentType === 'bar' ? 'pie' : 'bar';
            const icon = targetType === 'pie' ? '🥧' : '📊';
            return `Switch to ${icon} ${targetType.charAt(0).toUpperCase() + targetType.slice(1)} chart`;
        },

        // Recreate diary chart when aggregation mode changes
        // Uses the mode-specific chart type key (diaryWeekday, diaryMonth, diaryYear)
        onDiaryModeChange() {
            const diaryCategory = 'diary' + this.diaryAggregationMode.charAt(0).toUpperCase() + this.diaryAggregationMode.slice(1);
            this.$nextTick(() => {
                this.recreateChartForCategory(diaryCategory);
            });
        },

        // Recreate financial chart when view mode changes
        onFinancialModeChange() {
            this.$nextTick(() => {
                this.recreateChartForCategory('financial');
            });
        },
        
        // Handle folder selection via hidden file input
        handleFolderSelect(event) {
            // The hidden file input is used to trigger folder selection via pywebview
            // This handler is kept for compatibility but folder selection is done via the button
            console.log('Folder selection handled via pywebview API');
        },
        
        // Start folder selection via pywebview API
        async selectFolderAndAnalyze() {
            this.isAnalyzing = true;
            this.progressPercent = 0;
            this.analysisProgress = 'Selecting folder...';
            this.hasResults = false;
            
            try {
                if (window.pywebview && window.pywebview.api) {
                    // Open folder selection dialog in Python
                    const folderResult = await window.pywebview.api.select_folder();
                    
                    if (!folderResult.success) {
                        this.onAnalysisError(folderResult.error || 'Failed to select folder');
                        return;
                    }
                    
                    const folderPath = folderResult.folder_path;
                    this.selectedFolderName = folderPath.split(/[\\/]/).pop();
                    this.analysisProgress = 'Validating folder...';
                    
                    // Start analysis with the selected folder
                    window.pywebview.api.analyze_folder(folderPath).then((result) => {
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
            } catch (error) {
                this.onAnalysisError(error.message || 'Failed to select folder');
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
        
        // Load saved LePrAn snapshot data (JSON format)
        async loadSavedSnapshot() {
            this.isAnalyzing = true;
            this.progressPercent = 0;
            this.analysisProgress = 'Loading snapshot...';
            
            try {
                if (window.pywebview && window.pywebview.api) {
                    // Open file dialog in Python and load the JSON snapshot
                    const result = await window.pywebview.api.load_snapshot();
                    this.isAnalyzing = false;
                    this.progressPercent = 0;
                    this.analysisProgress = '';
                    
                    if (result && result.success) {
                        const r = result.result || {};
                        
                        // Update stats display from imported data
                        const savedTotalHours = r.total_hours || this.stats.totalHours || 0;
                        this.stats = {
                            username: r.username || this.stats.username || 'Imported',
                            filmsCount: r.films_count || this.stats.filmsCount || 0,
                            totalRuntime: this.formatRuntime(savedTotalHours),
                            totalHours: savedTotalHours,
                            totalDays: savedTotalHours / 24,
                            scrapedAt: r.scraped_at || this.stats.scrapedAt || 'Imported'
                        };
                        
                        // Update category data from imported analytics (top-level in result)
                        this.rawData.countries = this.parseDictResult(JSON.stringify(r.country_stats || {}));
                        this.rawData.languages = this.parseDictResult(JSON.stringify(r.language_stats || {}));
                        this.rawData.genres = this.parseDictResult(JSON.stringify(r.genre_stats || {}));
                        this.rawData.directors = this.parseDictResult(JSON.stringify(r.director_stats || {}));
                        this.rawData.actors = this.parseDictResult(JSON.stringify(r.actor_stats || {}));
                        this.rawData.decades = this.parseDictResult(JSON.stringify(r.decade_stats || {}));
                        
                        // Load diary data from snapshot (new) - convert dict to array format
                        if (r.diary_data) {
                            this.rawDiaryData = {
                                weekday: this._dictToDiaryArray(r.diary_data.weekday || {}),
                                month: this._dictToDiaryArray(r.diary_data.month || {}),
                                year: this._dictToDiaryArray(r.diary_data.year || {})
                            };
                        } else {
                            this.rawDiaryData = { weekday: [], month: [], year: [] };
                        }
                        
                        // Load financial data from snapshot (new) - convert dict to array format
                        if (r.financial_data) {
                            this.rawFinancialData = {
                                budget: this._dictToFinancialArray(r.financial_data.budget || {}),
                                boxoffice: this._dictToFinancialArray(r.financial_data.boxoffice || {})
                            };
                        } else {
                            this.rawFinancialData = { budget: [], boxoffice: [] };
                        }
                        
                        // Initialize charts after DOM updates
                        this.$nextTick(() => {
                            this.initCharts();
                        });
                        
                        this.hasResults = true;
                    } else {
                        alert('Failed to load snapshot: ' + (result?.error || 'Unknown error'));
                    }
                } else {
                    // Demo mode - use a sample file picker
                    this.simulateLoadCSV();
                }
            } catch (error) {
                this.isAnalyzing = false;
                this.progressPercent = 0;
                this.analysisProgress = '';
                this.onAnalysisError(error.message || 'Failed to load snapshot');
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
                const totalHours = result.total_hours || 0;
                this.stats = {
                    username: result.username || 'Unknown',
                    filmsCount: result.films_count || 0,
                    totalRuntime: this.formatRuntime(totalHours),
                    totalHours: totalHours,
                    totalDays: totalHours / 24,
                    scrapedAt: result.scraped_at || new Date().toLocaleDateString()
                };
                
                // Store category data
                this.rawData.countries = this.parseDictResult(result.countries);
                this.rawData.languages = this.parseDictResult(result.languages);
                this.rawData.genres = this.parseDictResult(result.genres);
                this.rawData.directors = this.parseDictResult(result.directors);
                this.rawData.actors = this.parseDictResult(result.actors);
                this.rawData.decades = this.parseDictResult(result.decades);
                
                // Load diary data (new) - convert dict to array format
                if (result.diary_data) {
                    this.rawDiaryData = {
                        weekday: this._dictToDiaryArray(result.diary_data.weekday || {}),
                        month: this._dictToDiaryArray(result.diary_data.month || {}),
                        year: this._dictToDiaryArray(result.diary_data.year || {})
                    };
                } else {
                    this.rawDiaryData = { weekday: [], month: [], year: [] };
                }
                
                // Load financial data (new) - convert dict to array format
                if (result.financial_data) {
                    this.rawFinancialData = {
                        budget: this._dictToFinancialArray(result.financial_data.budget || {}),
                        boxoffice: this._dictToFinancialArray(result.financial_data.boxoffice || {})
                    };
                } else {
                    this.rawFinancialData = { budget: [], boxoffice: [] };
                }
                
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
        
        // Convert diary dict (e.g. {Monday: 12, Friday: 25}) to array format
        _dictToDiaryArray(dictData) {
            if (!dictData || typeof dictData !== 'object') return [];
            const total = Object.values(dictData).reduce((s, v) => s + (Number(v) || 0), 0);
            return Object.entries(dictData)
                .map(([name, count]) => ({
                    name: String(name),
                    count: Number(count) || 0,
                    percent: total > 0 ? ((Number(count) || 0) / total * 100).toFixed(1) + '%' : '0.0%'
                }))
                .filter(item => item.count > 0);
        },
        
        // Convert financial dict (e.g. {Inception: 500000000, ...}) to array format
        _dictToFinancialArray(dictData) {
            if (!dictData || typeof dictData !== 'object') return [];
            const total = Object.values(dictData).reduce((s, v) => s + (Number(v) || 0), 0);
            return Object.entries(dictData)
                .map(([name, count]) => ({
                    name: String(name),
                    count: Number(count) || 0,
                    percent: total > 0 ? ((Number(count) || 0) / total * 100).toFixed(1) + '%' : '0.0%'
                }))
                .filter(item => item.count > 0);
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
        
        // Format total hours into days (display-only)
        formatDays(days) {
            if (!days || days === 0) return '~ 0.00 days';
            const d = days / 24;
            return '~ ' + d.toFixed(2) + ' days';
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
        
        // Toggle category expansion methods
        toggleCountries() {
            this.countriesExpanded = !this.countriesExpanded;
        },
        
        toggleLanguages() {
            this.languagesExpanded = !this.languagesExpanded;
        },
        
        toggleGenres() {
            this.genresExpanded = !this.genresExpanded;
        },
        
        toggleDirectors() {
            this.directorsExpanded = !this.directorsExpanded;
        },
        
        toggleActors() {
            this.actorsExpanded = !this.actorsExpanded;
        },
        
        toggleDecades() {
            this.decadesExpanded = !this.decadesExpanded;
        },
        
        toggleDiary() {
            this.diaryExpanded = !this.diaryExpanded;
        },
        
        toggleFinancial() {
            this.financialExpanded = !this.financialExpanded;
        },
        
         // Reset to input screen for new analysis
         resetToInput() {
             // Clean up polling interval if still active
             if (this._progressInterval) {
                 clearInterval(this._progressInterval);
                 this._progressInterval = null;
             }
             
             this.hasResults = false;
             this.selectedFolder = null;
             this.selectedFolderName = '';
             this.stats = { username: '', filmsCount: 0, totalRuntime: '0h', totalHours: 0, totalDays: 0, scrapedAt: '' };
            this.rawData = { countries: [], languages: [], genres: [], directors: [], actors: [], decades: [] };
            
            // Destroy charts to free memory
            this.destroyCharts();
        },
        
        // Save complete application state as JSON snapshot
        async saveResults() {
            if (!this.hasResults) return;
            
            try {
                if (window.pywebview && window.pywebview.api) {
                    const result = await window.pywebview.api.save_snapshot({
                        username: this.stats.username,
                        films_count: this.stats.filmsCount,
                        total_hours: this.stats.totalHours,
                        scraped_at: this.stats.scrapedAt,
                        countries: this.rawData.countries,
                        languages: this.rawData.languages,
                        genres: this.rawData.genres,
                        directors: this.rawData.directors,
                        actors: this.rawData.actors,
                        decades: this.rawData.decades
                    });
                    
                    if (result && result.success) {
                        alert('Snapshot saved successfully to:\n' + result.file_path);
                    } else {
                        alert('Failed to save snapshot: ' + (result?.error || 'Unknown error'));
                    }
                } else {
                    // Demo mode - download as JSON
                    this.downloadDemoSave();
                }
            } catch (error) {
                alert('Error saving snapshot: ' + (error.message || 'Unknown error'));
            }
        },
        
        // Prepare chart data based on type (bar or pie)
        prepareChartData(data, chartType, limit) {
            const sorted = [...data].sort((a, b) => b.count - a.count);
            
            if (chartType === 'pie' && sorted.length > limit) {
                // For pie charts: top N slices + "Other" for the rest
                const topItems = sorted.slice(0, limit);
                const otherCount = sorted.slice(limit).reduce((sum, item) => sum + item.count, 0);
                
                return {
                    labels: [...topItems.map(item => item.name), 'Other'],
                    counts: [...topItems.map(item => item.count), otherCount]
                };
            } else {
                // For bar charts: top 10 items (no "Other" grouping)
                const items = sorted.slice(0, limit);
                return {
                    labels: items.map(item => item.name),
                    counts: items.map(item => item.count)
                };
            }
        },
        
        // Convert HSL to Hex color string
        hslToHex(h, s, l) {
            l /= 100;
            const a = s * Math.min(l, 1 - l) / 100;
            const f = n => {
                const run = n + (h / 30) % 12;
                const color = l - a * Math.max(Math.min(run - 3, 9 - run, 1), -1);
                return Math.round(255 * color).toString(16).padStart(2, '0');
            };
            return `#${f(0)}${f(8)}${f(4)}`;
        },
        
        // Generate highly distinct colors for pie chart slices
        // Uses a predefined palette of well-separated colors for maximum visual contrast
        generatePieColors(baseColor, count) {
            // Predefined palette of highly distinct colors (hex without #)
            // These are spaced to ensure adjacent slices are visually different
            const distinctColors = [
                { bg: '#FF6B6B', border: '#FF5252' },  // Red
                { bg: '#4ECDC4', border: '#26A69A' },  // Teal
                { bg: '#45B7D1', border: '#2196F3' },  // Blue
                { bg: '#FFA726', border: '#FB8C00' },  // Orange
                { bg: '#AB47BC', border: '#8E24AA' },  // Purple
                { bg: '#66BB6A', border: '#43A047' },  // Green
                { bg: '#EC407A', border: '#D81B60' },  // Pink
                { bg: '#26C6DA', border: '#00ACC1' },  // Cyan
                { bg: '#FFEE58', border: '#FFC107' },  // Yellow
                { bg: '#8D6E63', border: '#6D4C41' },  // Brown
                { bg: '#7E57C2', border: '#5E35B1' },  // Indigo
                { bg: '#EF5350', border: '#E53935' },  // Coral
                { bg: '#29B6F6', border: '#039BE5' },  // Light Blue
                { bg: '#9CCC65', border: '#7CB342' },  // Lime
                { bg: '#FF7043', border: '#F4511E' },  // Deep Orange
                { bg: '#26A69A', border: '#00897B' },  // Green Teal
                { bg: '#FDD835', border: '#F9A825' },  // Gold
                { bg: '#5C6BC0', border: '#3949AB' },  // Blue Indigo
                { bg: '#C0CA33', border: '#9E9D24' },  // Lime Green
                { bg: '#EF5350', border: '#AD1457' },  // Crimson
            ];
            
            const backgrounds = [];
            const borders = [];
            
            for (let i = 0; i < count; i++) {
                if (i < distinctColors.length) {
                    backgrounds.push(distinctColors[i].bg + 'cc'); // ~80% opacity
                    borders.push(distinctColors[i].border);
                } else {
                    // If we need more colors than our palette, generate from base
                    const r = parseInt(baseColor.slice(1, 3), 16) / 255;
                    const g = parseInt(baseColor.slice(3, 5), 16) / 255;
                    const b = parseInt(baseColor.slice(5, 7), 16) / 255;
                    const max = Math.max(r, g, b), min = Math.min(r, g, b);
                    let h, s, l = (max + min) / 2;
                    if (max === min) {
                        h = s = 0;
                    } else {
                        const d = max - min;
                        s = l > 0.5 ? d / (2 - max - min) : d / (max + min);
                        switch (max) {
                            case r: h = ((g - b) / d + (g < b ? 6 : 0)) / 6; break;
                            case g: h = ((b - r) / d + 2) / 6; break;
                            case b: h = ((r - g) / d + 4) / 6; break;
                        }
                    }
                    // Use larger hue step for more separation
                    const hueStep = 137; // Golden angle approximation for better distribution
                    const hue = Math.round((h * 360 + hueStep * i) % 360);
                    const sat = 70 + (i % 3) * 10; // Vary saturation
                    const lit = 50 + (i % 2) * 15; // Vary lightness
                    const hex = this.hslToHex(hue, sat, lit);
                    backgrounds.push(hex + 'cc');
                    borders.push(this.hslToHex(hue, sat, lit - 10));
                }
            }
            
            return { backgrounds, borders };
        },
        
        // Initialize Chart.js charts for all panels
        initCharts() {
            this.destroyCharts();
            
            const chartConfigs = [
                { id: 'countriesChart', data: this.rawData.countries, color: '#58a6ff', category: 'countries' },
                { id: 'languagesChart', data: this.rawData.languages, color: '#3fb950', category: 'languages' },
                { id: 'genresChart', data: this.rawData.genres, color: '#bc8cff', category: 'genres' },
                { id: 'directorsChart', data: this.rawData.directors, color: '#d29922', category: 'directors' },
                { id: 'actorsChart', data: this.rawData.actors, color: '#f85149', category: 'actors' },
                { id: 'decadesChart', data: this.rawData.decades, color: '#f0883e', category: 'decades' },
                { id: 'diaryChart', data: this.sortedDiaryData, color: '#58a6ff', category: 'diaryWeekday' },
                { id: 'financialChart', data: this.sortedFinancialData, color: '#3fb950', category: 'financial' }
            ];
            
            chartConfigs.forEach(config => {
                this._createChart(config);
            });
        },
        
        // Create a single chart with proper error handling
        _createChart(config) {
            const canvas = document.getElementById(config.id);
            if (!canvas || !config.data.length) return;
            
            // Safety check: destroy any existing Chart.js instance attached to this canvas
            const existingChart = Chart.getChart(canvas);
            if (existingChart) {
                existingChart.destroy();
            }
            
            const chartType = this.chartTypes[config.category] || 'bar';
            const barLimit = 10;
            const pieLimit = this.pieSliceLimit;
            const chartData = this.prepareChartData(config.data, chartType, chartType === 'pie' ? pieLimit : barLimit);
            
            let chartInstance;
            
            try {
                if (chartType === 'pie') {
                    const pieColors = this.generatePieColors(config.color, chartData.labels.length);
                    
                    const chartOptions = {
                        responsive: true,
                        maintainAspectRatio: false,
                        plugins: {
                            legend: {
                                position: 'bottom',
                                labels: {
                                    color: '#8b949e',
                                    font: { size: 10 },
                                    padding: 8,
                                    boxWidth: 12
                                }
                            },
                            tooltip: {
                                backgroundColor: '#21262d',
                                titleColor: '#e6edf3',
                                bodyColor: '#8b949e',
                                borderColor: '#30363d',
                                borderWidth: 1,
                                padding: 10,
                                displayColors: true
                            }
                        }
                    };
                    
                    chartInstance = new Chart(canvas, {
                        type: 'pie',
                        data: {
                            labels: chartData.labels,
                            datasets: [{
                                label: 'Films',
                                data: chartData.counts,
                                backgroundColor: pieColors.backgrounds,
                                borderColor: pieColors.borders,
                                borderWidth: 1
                            }]
                        },
                        options: chartOptions
                    });
                } else {
                    const chartOptions = {
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
                                    font: { size: 11 },
                                    autoSkip: false
                                }
                            }
                        }
                    };
                    
                    chartInstance = new Chart(canvas, {
                        type: 'bar',
                        data: {
                            labels: chartData.labels,
                            datasets: [{
                                label: 'Films',
                                data: chartData.counts,
                                backgroundColor: config.color + '99',
                                borderColor: config.color,
                                borderWidth: 1,
                                borderRadius: 4
                            }]
                        },
                        options: chartOptions
                    });
                }
                
                // Store the chart instance only if creation succeeded
                if (chartInstance && !chartInstance.destroyed) {
                    this.charts[config.id] = chartInstance;
                }
            } catch (error) {
                console.error(`Failed to create chart ${config.id}:`, error);
            }
        },
        
        // Destroy all chart instances
        destroyCharts() {
            for (const id in this.charts) {
                try {
                    if (this.charts[id] && !this.charts[id].destroyed) {
                        this.charts[id].destroy();
                    }
                } catch (e) {
                    console.warn(`Error destroying chart ${id}:`, e);
                }
            }
            // Clear all chart references
            this.charts = {};
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
                }),
                decades: JSON.stringify({
                    '2020s': 15,
                    '2010s': 42,
                    '2000s': 38,
                    '1990s': 28,
                    '1980s': 12,
                    '1970s': 5,
                    '1960s': 2
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
                actors: this.rawData.actors,
                decades: this.rawData.decades
            };
            
            const blob = new Blob([JSON.stringify(data, null, 2)], { type: 'application/json' });
            const url = URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            a.download = `${this.stats.username}_lepran_results.json`;
            a.click();
            URL.revokeObjectURL(url);
        },
        
        // Helper to get country dictionary from imported data (for snapshot import)
        _getCountryDict() {
            // Try to extract from analytics if available
            return {};
        },
        _getLangDict() { return {}; },
        _getGenreDict() { return {}; },
        _getDirectorDict() { return {}; },
        _getActorDict() { return {}; }
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