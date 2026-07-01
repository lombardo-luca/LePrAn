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
          
          // Theme state
          currentTheme: 'dark',
          
          // TMDB API Key Modal state
          showApiKeyModal: false,
          apiKeyInput: '',
          apiKeyError: '',
          apiKeyValidating: false,
          
          // Import options (default: watchlist is enabled = opt-out behavior)
          importWatchlist: true,
         
          // Detailed progress stats
          filmsProcessed: 0,
          filmsTotal: 0,
          processingSpeed: 0,
          etaSeconds: 0,
          
          // Step indicator state
          currentStep: 0,
          totalSteps: 0,
          status: '',
        
        // Reference to this app instance for global callback
        _appRef: null,
        
        // Charts instances
        charts: {},
        
          // Per-panel data source toggle: each panel independently tracks 'watched' or 'watchlist'
          // Diary panel is excluded (no toggle) - it always uses diary data
          panelDataSources: {
              films: 'watched',
              people: 'watched',
              finance: 'watched'
          },
          
          // Backward compatibility computed property for legacy code
          get dataSource() {
              return this.panelDataSources.films;
          },
          set dataSource(val) {
              // Set ALL panels (for backward compatibility)
              this.panelDataSources.films = val;
              this.panelDataSources.people = val;
              this.panelDataSources.finance = val;
          },
          
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
          rightPanel: 'directors',
         
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
           budgetRangeExpanded: false,
         
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
           rawBudgetRangeData: [],
           
           // Watchlist data storage (parallel to rawData for watched data)
           watchlistData: {
               countries: [],
               languages: [],
               genres: [],
               directors: [],
               actors: [],
               decades: []
           },
           
           // Watchlist financial data storage
           watchlistFinancialData: {
               budget: [],
               boxoffice: []
           },
           watchlistBudgetRangeData: [],
           
           // Watchlist stats (separate from watched stats)
           watchlistStats: {
               username: '',
               filmsCount: 0,
               totalRuntime: '0h',
               totalHours: 0,
               totalDays: 0,
               scrapedAt: ''
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
                financial: 'bar',
                budgetRange: 'bar'
            },
          
          // Diary aggregation mode: 'weekday', 'month', or 'year'
          diaryAggregationMode: 'weekday',
          
           // Financial view mode: 'budget', 'budget_range', or 'boxoffice'
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
          // Source-aware: each category uses its panel's data source
          // Film panel categories (countries, languages, genres, decades) -> panelDataSources.films
          // People panel categories (directors, actors) -> panelDataSources.people
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
              
              // Film panel categories: countries, languages, genres, decades
              const filmCategories = ['countries', 'languages', 'genres', 'decades'];
              // People panel categories: directors, actors
              const peopleCategories = ['directors', 'actors'];
              
              // Choose data source based on each panel's toggle
              const filmSourceData = this.panelDataSources.films === 'watchlist' ? this.watchlistData : this.rawData;
              const peopleSourceData = this.panelDataSources.people === 'watchlist' ? this.watchlistData : this.rawData;
              
              for (const key of Object.keys(filmSourceData)) {
                  const source = filmCategories.includes(key) ? filmSourceData : peopleSourceData;
                  let sorted = [...source[key]].sort((a, b) => b.count - a.count);
                  
                  // Limit to top 100 unless that category is expanded
                  if (!expansionStates[key]) {
                      sorted = sorted.slice(0, this.DISPLAY_LIMIT);
                  }
                  
                  result[key] = sorted;
              }
              return result;
          },
          
          sourceLengthForCategory(category) {
              const filmCategories = ['countries', 'languages', 'genres', 'decades'];
              const sourceName = filmCategories.includes(category)
                  ? this.panelDataSources.films
                  : this.panelDataSources.people;
              const sourceData = sourceName === 'watchlist' ? this.watchlistData : this.rawData;
              return (sourceData[category] || []).length;
          },
          
          sourceLengthForFinancial() {
              if (this.financialViewMode === 'budget_range') {
                  const sourceData = this.panelDataSources.finance === 'watchlist'
                      ? this.watchlistBudgetRangeData
                      : this.rawBudgetRangeData;
                  return sourceData.length;
              }
              const sourceData = this.panelDataSources.finance === 'watchlist'
                  ? this.watchlistFinancialData
                  : this.rawFinancialData;
              return (sourceData[this.financialViewMode] || []).length;
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
           // Source-aware: reads from rawFinancialData (watched) or watchlistFinancialData based on panelDataSources.finance
           // Limited to DISPLAY_LIMIT entries unless financialExpanded is true
           get sortedFinancialData() {
               const sourceData = this.panelDataSources.finance === 'watchlist' 
                   ? this.watchlistFinancialData 
                   : this.rawFinancialData;
               let data = sourceData[this.financialViewMode] || [];
               data = [...data].sort((a, b) => b.count - a.count);
               if (!this.financialExpanded) {
                   data = data.slice(0, this.DISPLAY_LIMIT);
               }
               return data;
           },
           
           // Computed: sorted budget range data (bucket aggregation)
           // Source-aware: reads from rawBudgetRangeData (watched) or watchlistBudgetRangeData based on panelDataSources.finance
            get sortedBudgetRangeData() {
                const sourceData = this.panelDataSources.finance === 'watchlist' 
                    ? this.watchlistBudgetRangeData 
                    : this.rawBudgetRangeData;
                let data = [...sourceData];
                // Sort by number of films descending, "Unknown / Not reported" last
                data.sort((a, b) => {
                    if (a.range === 'Unknown / Not reported') return 1;
                    if (b.range === 'Unknown / Not reported') return -1;
                    return b.count - a.count;
                });
                if (!this.budgetRangeExpanded) {
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
            
            // Load saved theme preference
            this.loadThemePreference();
            
            // Check if we're running in pywebview
            if (window.pywebview) {
                console.log('pywebview API detected');
                this.checkApiKey();
            } else {
                console.warn('Running without pywebview bridge - using demo mode');
                window.addEventListener('pywebviewready', () => {
                    console.log('pywebview ready event fired');
                    this.checkApiKey();
                });
            }
        },
        
        // --- Theme Methods ---
        
        // Load theme preference from backend API
        loadThemePreference() {
            try {
                if (window.pywebview && window.pywebview.api) {
                    window.pywebview.api.get_theme().then((theme) => {
                        if (theme === 'light' || theme === 'dark') {
                            this.currentTheme = theme;
                            this.applyTheme();
                        }
                    }).catch((err) => {
                        console.warn('Failed to load theme preference:', err);
                        this.applyTheme();
                    });
                } else {
                    // Fallback: check localStorage (legacy)
                    const saved = localStorage.getItem('lepran_theme');
                    if (saved === 'light' || saved === 'dark') {
                        this.currentTheme = saved;
                    }
                    this.applyTheme();
                }
            } catch (e) {
                console.warn('Failed to load theme preference:', e);
                this.applyTheme();
            }
        },
        
        // Apply theme to document
        applyTheme() {
            document.documentElement.setAttribute('data-theme', this.currentTheme);
            // Save to localStorage as fallback
            try {
                localStorage.setItem('lepran_theme', this.currentTheme);
            } catch (e) { /* ignore */ }
            // Recreate all charts with new theme colors
            this.$nextTick(() => {
                this.recreateAllChartsForTheme();
            });
        },
        
        // Toggle between dark and light theme
        toggleTheme() {
            this.currentTheme = this.currentTheme === 'dark' ? 'light' : 'dark';
            this.applyTheme();
            // Persist to backend API
            if (window.pywebview && window.pywebview.api) {
                window.pywebview.api.set_theme(this.currentTheme).catch((err) => {
                    console.warn('Failed to save theme preference:', err);
                });
            }
        },
        
        // Recreate all charts with theme-aware colors
        recreateAllChartsForTheme() {
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
                financial: 'financialChart',
                budgetRange: 'budgetRangeChart'
            };
            for (const category of Object.keys(categoryToChartId)) {
                this.recreateChartForCategory(category);
            }
        },

        // TMDB API Key handling
        async checkApiKey() {
            try {
                if (window.pywebview && window.pywebview.api) {
                    const result = await window.pywebview.api.check_tmdb_key();
                    if (!result.has_key) {
                        this.showApiKeyModal = true;
                    }
                }
            } catch (error) {
                console.error("Error checking TMDB key:", error);
            }
        },
        
        async submitApiKey() {
            if (!this.apiKeyInput.trim()) return;
            
            this.apiKeyValidating = true;
            this.apiKeyError = '';
            
            try {
                if (window.pywebview && window.pywebview.api) {
                    // Validate key first
                    const validation = await window.pywebview.api.validate_tmdb_api_key(this.apiKeyInput);
                    if (!validation.valid) {
                        this.apiKeyError = validation.error || "Invalid API key.";
                        this.apiKeyValidating = false;
                        return;
                    }
                    
                    // Save key
                    const result = await window.pywebview.api.save_tmdb_api_key(this.apiKeyInput);
                    if (result.success) {
                        this.showApiKeyModal = false;
                    } else {
                        this.apiKeyError = result.error || "Failed to save API key.";
                    }
                }
            } catch (error) {
                console.error("Error saving API key:", error);
                this.apiKeyError = "An unexpected error occurred.";
            } finally {
                this.apiKeyValidating = false;
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
                financial: 'financialChart',
                budgetRange: 'budgetRangeChart'
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
            const filmSourceData = this.panelDataSources.films === 'watchlist' ? this.watchlistData : this.rawData;
            const peopleSourceData = this.panelDataSources.people === 'watchlist' ? this.watchlistData : this.rawData;
            const dataMap = {
                countries: filmSourceData.countries,
                languages: filmSourceData.languages,
                genres: filmSourceData.genres,
                directors: peopleSourceData.directors,
                actors: peopleSourceData.actors,
                decades: filmSourceData.decades,
                diaryWeekday: this.sortedDiaryData,
                diaryMonth: this.sortedDiaryData,
                diaryYear: this.sortedDiaryData,
                financial: this.sortedFinancialData,
                budgetRange: this.sortedBudgetRangeData
            };
            
            // Theme-aware color maps
            const darkColorMap = {
                countries: '#58a6ff',
                languages: '#3fb950',
                genres: '#bc8cff',
                directors: '#d29922',
                actors: '#f85149',
                decades: '#f0883e',
                diaryWeekday: '#58a6ff',
                diaryMonth: '#58a6ff',
                diaryYear: '#58a6ff',
                financial: '#3fb950',
                budgetRange: '#bc8cff'
            };
            
            const lightColorMap = {
                countries: '#0969da',
                languages: '#1a7f37',
                genres: '#8250df',
                directors: '#bf6600',
                actors: '#cf222e',
                decades: '#d17900',
                diaryWeekday: '#0969da',
                diaryMonth: '#0969da',
                diaryYear: '#0969da',
                financial: '#1a7f37',
                budgetRange: '#8250df'
            };
            
            const colorMap = this.currentTheme === 'light' ? lightColorMap : darkColorMap;
            
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
                if (this.financialViewMode === 'budget_range') {
                    this.recreateChartForCategory('budgetRange');
                } else {
                    this.recreateChartForCategory('financial');
                }
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
                    
                    // Start analysis with the selected folder and importWatchlist flag
                    window.pywebview.api.analyze_folder(folderPath, this.importWatchlist).then((result) => {
                        if (result.success === false && result.error === 'tmdb_key_missing') {
                            this.isAnalyzing = false;
                            this.showApiKeyModal = true;
                            return;
                        }
                        if (result.status === 'started') {
                            // Start polling for progress
                            this._startProgressPolling();
                        } else if (result.success) {
                            // Count-only result returned directly
                            this.onAnalysisComplete(result);
                        } else {
                            this.onAnalysisError(result.error || 'Failed to start analysis');
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
                    this.status = progress.status;
                    this.analysisProgress = progress.status;
                    
                    // Parse step indicator from status (format: "Step X/Y: ...")
                    const stepMatch = progress.status.match(/^Step\s+(\d+)\/(\d+)/);
                    if (stepMatch) {
                        this.currentStep = parseInt(stepMatch[1]);
                        this.totalSteps = parseInt(stepMatch[2]);
                    } else {
                        this.currentStep = 0;
                        this.totalSteps = 0;
                    }
                    
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
                        const savedTotalDays = r.total_days != null ? r.total_days : savedTotalHours / 24;
                        this.stats = {
                            username: r.username || this.stats.username || 'Imported',
                            filmsCount: r.films_count || this.stats.filmsCount || 0,
                            totalRuntime: this.formatRuntime(savedTotalHours),
                            totalHours: savedTotalHours,
                            totalDays: savedTotalDays,
                            scrapedAt: r.scraped_at || this.stats.scrapedAt || 'Imported'
                        };
                        
                        // Update category data from imported analytics (top-level in result)
                        // FIX: Pass dict directly (not wrapped in JSON.stringify) to avoid double-serialization
                        this.rawData.countries = this.parseDictResult(r.country_stats || {}, r.films_count);
                        this.rawData.languages = this.parseDictResult(r.language_stats || {}, r.films_count);
                        this.rawData.genres = this.parseDictResult(r.genre_stats || {}, r.films_count);
                        this.rawData.directors = this.parseDictResult(r.director_stats || {}, r.films_count);
                        this.rawData.actors = this.parseDictResult(r.actor_stats || {}, r.films_count);
                        this.rawData.decades = this.parseDictResult(r.decade_stats || {}, r.films_count);
                        
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
                            // Load budget range data from snapshot (new)
                            this.rawBudgetRangeData = this._dictToBudgetRangeArray(r.financial_data.budget_range || {});
                        } else {
                            this.rawFinancialData = { budget: [], boxoffice: [] };
                            this.rawBudgetRangeData = [];
                        }
                        
                        this.loadWatchlistData(r);
                        
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
                const totalDays = result.total_days != null ? result.total_days : totalHours / 24;
                this.stats = {
                    username: result.username || 'Unknown',
                    filmsCount: result.films_count || 0,
                    totalRuntime: this.formatRuntime(totalHours),
                    totalHours: totalHours,
                    totalDays: totalDays,
                    scrapedAt: result.scraped_at || new Date().toLocaleDateString()
                };
                
                // Store category data - pass films_count for correct percentage calculation
                this.rawData.countries = this.parseDictResult(result.countries, result.films_count);
                this.rawData.languages = this.parseDictResult(result.languages, result.films_count);
                this.rawData.genres = this.parseDictResult(result.genres, result.films_count);
                this.rawData.directors = this.parseDictResult(result.directors, result.films_count);
                this.rawData.actors = this.parseDictResult(result.actors, result.films_count);
                this.rawData.decades = this.parseDictResult(result.decades, result.films_count);
                
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
                    // Load budget range data (new)
                    this.rawBudgetRangeData = this._dictToBudgetRangeArray(result.financial_data.budget_range || {});
                } else {
                    this.rawFinancialData = { budget: [], boxoffice: [] };
                    this.rawBudgetRangeData = [];
                }
                
                this.loadWatchlistData(result);
                
                // Initialize charts after DOM updates
                this.$nextTick(() => {
                    this.initCharts();
                });
            } else {
                alert('Analysis failed: ' + (result.error || 'Unknown error'));
                this.hasResults = false;
            }
        },
        

        // Load watchlist analytics into the isolated watchlist stores.
        loadWatchlistData(result) {
            const resetWatchlist = () => {
                this.watchlistStats = { username: '', filmsCount: 0, totalRuntime: '0h', totalHours: 0, totalDays: 0, scrapedAt: '' };
                this.watchlistData = { countries: [], languages: [], genres: [], directors: [], actors: [], decades: [] };
                this.watchlistFinancialData = { budget: [], boxoffice: [] };
                this.watchlistBudgetRangeData = [];
                for (const panel of Object.keys(this.panelDataSources)) {
                    if (this.panelDataSources[panel] === 'watchlist') {
                        this.panelDataSources[panel] = 'watched';
                    }
                }
            };
            
            if (result.watchlist_data) {
                const wd = result.watchlist_data;
                this.watchlistStats = {
                    username: '',
                    filmsCount: wd.films_count || 0,
                    totalRuntime: this.formatRuntime(wd.total_hours || 0),
                    totalHours: wd.total_hours || 0,
                    totalDays: wd.total_hours ? wd.total_hours / 24 : 0,
                    scrapedAt: ''
                };
                this.watchlistData.countries = this.parseDictResult(wd.countries || {}, wd.films_count);
                this.watchlistData.languages = this.parseDictResult(wd.languages || {}, wd.films_count);
                this.watchlistData.genres = this.parseDictResult(wd.genres || {}, wd.films_count);
                this.watchlistData.directors = this.parseDictResult(wd.directors || {}, wd.films_count);
                this.watchlistData.actors = this.parseDictResult(wd.actors || {}, wd.films_count);
                this.watchlistData.decades = this.parseDictResult(wd.decades || {}, wd.films_count);
                
                if (wd.financial_data) {
                    this.watchlistFinancialData = {
                        budget: this._dictToFinancialArray(wd.financial_data.budget || {}),
                        boxoffice: this._dictToFinancialArray(wd.financial_data.boxoffice || {})
                    };
                    this.watchlistBudgetRangeData = this._dictToBudgetRangeArray(wd.financial_data.budget_range || {});
                } else {
                    this.watchlistFinancialData = { budget: [], boxoffice: [] };
                    this.watchlistBudgetRangeData = [];
                }
                return;
            }
            
            if (result.watchlist_analytics) {
                const wa = result.watchlist_analytics;
                this.watchlistStats = {
                    username: wa.username || this.watchlistStats.username || 'Watchlist',
                    filmsCount: wa.total_films || 0,
                    totalRuntime: this.formatRuntime(wa.total_hours || 0),
                    totalHours: wa.total_hours || 0,
                    totalDays: wa.total_hours ? wa.total_hours / 24 : 0,
                    scrapedAt: wa.scraped_at || this.watchlistStats.scrapedAt || ''
                };
                this.watchlistData.countries = this.parseDictResult(wa.country_stats || {}, wa.total_films);
                this.watchlistData.languages = this.parseDictResult(wa.language_stats || {}, wa.total_films);
                this.watchlistData.genres = this.parseDictResult(wa.genre_stats || {}, wa.total_films);
                this.watchlistData.directors = this.parseDictResult(wa.director_stats || {}, wa.total_films);
                this.watchlistData.actors = this.parseDictResult(wa.actor_stats || {}, wa.total_films);
                this.watchlistData.decades = this.parseDictResult(wa.decade_stats || {}, wa.total_films);
                
                if (wa.financial_data) {
                    this.watchlistFinancialData = {
                        budget: this._dictToFinancialArray(wa.financial_data.budget || {}),
                        boxoffice: this._dictToFinancialArray(wa.financial_data.boxoffice || {})
                    };
                    this.watchlistBudgetRangeData = this._dictToBudgetRangeArray(wa.financial_data.budget_range || {});
                } else {
                    this.watchlistFinancialData = { budget: [], boxoffice: [] };
                    this.watchlistBudgetRangeData = [];
                }
                return;
            }
            
            resetWatchlist();
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
        // Optional filmsCount parameter for correct percentage calculation
        parseDictResult(dictData, filmsCount) {
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
                    count: Number(count) || 0,
                    percent: this.calculatePercentForCount(Number(count) || 0, filmsCount)
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
        
        // Convert budget range dict to array format
        // Expected format: { buckets: [{range, start, end, count}, ...], totalFilmsWithBudget: N }
        _dictToBudgetRangeArray(dictData) {
            if (!dictData || typeof dictData !== 'object') return [];
            if (Array.isArray(dictData.buckets)) {
                const totalFilms = dictData.totalFilmsWithBudget || 1;
                return dictData.buckets.map(bucket => ({
                    range: bucket.range || 'Unknown',
                    start: bucket.start || 0,
                    end: bucket.end || 0,
                    count: bucket.count || 0,
                    percent: totalFilms > 0 ? ((bucket.count || 0) / totalFilms * 100).toFixed(1) + '%' : '0.0%'
                }));
            }
            return Object.entries(dictData)
                .filter(([range, count]) => range !== 'buckets' && range !== 'totalFilmsWithBudget' && Number(count) > 0)
                .map(([range, count]) => {
                    const match = range.match(/[\$]?([\d,]+)/g);
                    return {
                        range: String(range),
                        start: match ? (match[0] ? parseInt(match[0].replace(/[\$,]/g, '')) || 0 : 0) : 0,
                        end: match && match[1] ? parseInt(match[1].replace(/[\$,]/g, '')) || 0 : 0,
                        count: Number(count) || 0,
                        percent: '0.0%'
                    };
                });
        },
        
        // Calculate percentage for a count value (source-aware)
        calculatePercent(count) {
            const filmsCount = this.dataSource === 'watchlist' 
                ? this.watchlistStats.filmsCount 
                : this.stats.filmsCount;
            if (!filmsCount || filmsCount === 0) return '0.00%';
            const percent = (count / filmsCount) * 100;
            return percent.toFixed(2) + '%';
        },
        
        // Calculate percentage for a count value with explicit films count
        // Used by parseDictResult when loading data with known film counts
        calculatePercentForCount(count, filmsCount) {
            if (!filmsCount || filmsCount === 0) return '0.00%';
            const percent = (count / filmsCount) * 100;
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
        
        // Format total days into readable string (display-only)
        formatDays(days) {
            if (!days || days === 0) return '~ 0.00 days';
            return '~ ' + days.toFixed(2) + ' days';
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
        
        toggleBudgetRange() {
            this.budgetRangeExpanded = !this.budgetRangeExpanded;
        },
        

        setPanelDataSource(panel, source) {
            if (!Object.prototype.hasOwnProperty.call(this.panelDataSources, panel)) return;
            if (source !== 'watched' && source !== 'watchlist') return;
            if (source === 'watchlist' && !this.hasWatchlistData) return;
            this.panelDataSources[panel] = source;
            this.$nextTick(() => {
                this.refreshPanelCharts(panel);
            });
        },
        
        refreshPanelCharts(panel) {
            const panelCategories = {
                films: ['countries', 'languages', 'genres', 'decades'],
                people: ['directors', 'actors'],
                finance: ['financial', 'budgetRange']
            };
            const categories = panelCategories[panel] || [];
            categories.forEach(category => this.recreateChartForCategory(category));
        },
        
        togglePanelDataSource(panel) {
            const current = this.panelDataSources[panel] || 'watched';
            this.setPanelDataSource(panel, current === 'watchlist' ? 'watched' : 'watchlist');
        },
        
        // Switch data source between watched and watchlist
        // For backward compatibility: switches the Film panel (default)
        switchDataSource(source) {
            if (source === 'watched' || source === 'watchlist') {
                this.setPanelDataSource('films', source);
            }
        },
        
        // Get display label for current data source
        getDataSourceLabel() {
            return this.dataSource === 'watchlist' ? 'Watchlist' : 'Watched';
        },
        
        // Check if watchlist data is available (loaded from analysis AND import was requested)
        get hasWatchlistData() {
            return this.importWatchlist && this.watchlistStats && this.watchlistStats.filmsCount > 0;
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
             this.importWatchlist = true; // Reset to default
             this.stats = { username: '', filmsCount: 0, totalRuntime: '0h', totalHours: 0, totalDays: 0, scrapedAt: '' };
            this.rawData = { countries: [], languages: [], genres: [], directors: [], actors: [], decades: [] };
            this.rawDiaryData = { weekday: [], month: [], year: [] };
            this.rawFinancialData = { budget: [], boxoffice: [] };
            this.rawBudgetRangeData = [];
            this.loadWatchlistData({});
            this.panelDataSources = { films: 'watched', people: 'watched', finance: 'watched' };
            
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
                        decades: this.rawData.decades,
                        // Diary analytics
                        diary_data: {
                            weekday: Object.fromEntries(this.rawDiaryData.weekday.map(i => [i.name, i.count])),
                            month: Object.fromEntries(this.rawDiaryData.month.map(i => [i.name, i.count])),
                            year: Object.fromEntries(this.rawDiaryData.year.map(i => [i.name, i.count]))
                        },
                        // Financial analytics
                        financial_data: {
                            budget: Object.fromEntries(this.rawFinancialData.budget.map(i => [i.name, i.count])),
                            boxoffice: Object.fromEntries(this.rawFinancialData.boxoffice.map(i => [i.name, i.count])),
                            budget_range: Object.fromEntries(this.rawBudgetRangeData.map(i => [i.range, i.count]))
                        },
                        // Watchlist analytics (only if import was enabled)
                        ...(this.importWatchlist ? {
                            watchlist_data: {
                                films_count: this.watchlistStats.filmsCount,
                                total_hours: this.watchlistStats.totalHours,
                                countries: this.watchlistData.countries,
                                languages: this.watchlistData.languages,
                                genres: this.watchlistData.genres,
                                directors: this.watchlistData.directors,
                                actors: this.watchlistData.actors,
                                decades: this.watchlistData.decades,
                                financial_data: {
                                    budget: Object.fromEntries(this.watchlistFinancialData.budget.map(i => [i.name, i.count])),
                                    boxoffice: Object.fromEntries(this.watchlistFinancialData.boxoffice.map(i => [i.name, i.count])),
                                    budget_range: Object.fromEntries(this.watchlistBudgetRangeData.map(i => [i.range, i.count]))
                                }
                            }
                        } : {})
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
            
            // Budget range data uses 'range' property instead of 'name'
            const getLabel = (item) => (item.range !== undefined ? item.range : item.name);
            
            if (chartType === 'pie' && sorted.length > limit) {
                // For pie charts: top N slices + "Other" for the rest
                const topItems = sorted.slice(0, limit);
                const otherCount = sorted.slice(limit).reduce((sum, item) => sum + item.count, 0);
                
                return {
                    labels: [...topItems.map(getLabel), 'Other'],
                    counts: [...topItems.map(item => item.count), otherCount]
                };
            } else {
                // For bar charts: top 10 items (no "Other" grouping)
                const items = sorted.slice(0, limit);
                return {
                    labels: items.map(getLabel),
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
        // Source-aware: each panel uses its own data source selection
        // Film panel (countries, languages, genres, decades) -> panelDataSources.films
        // People panel (directors, actors) -> panelDataSources.people
        // Finance panel (financial, budgetRange) -> panelDataSources.finance
        // Diary panel -> always uses rawDiaryData (no toggle)
        initCharts() {
            this.destroyCharts();
            
            // Choose data sources based on each panel's toggle
            const filmSourceData = this.panelDataSources.films === 'watchlist' ? this.watchlistData : this.rawData;
            const peopleSourceData = this.panelDataSources.people === 'watchlist' ? this.watchlistData : this.rawData;
            const financeSourceData = this.panelDataSources.finance === 'watchlist' ? this.watchlistFinancialData : this.rawFinancialData;
            const financeBudgetRangeSource = this.panelDataSources.finance === 'watchlist' ? this.watchlistBudgetRangeData : this.rawBudgetRangeData;
            
            // Theme-aware color maps
            const darkColorMap = {
                countries: '#58a6ff',
                languages: '#3fb950',
                genres: '#bc8cff',
                directors: '#d29922',
                actors: '#f85149',
                decades: '#f0883e',
                diaryWeekday: '#58a6ff',
                diaryMonth: '#58a6ff',
                diaryYear: '#58a6ff',
                financial: '#3fb950',
                budgetRange: '#bc8cff'
            };
            
            const lightColorMap = {
                countries: '#0969da',
                languages: '#1a7f37',
                genres: '#8250df',
                directors: '#bf6600',
                actors: '#cf222e',
                decades: '#d17900',
                diaryWeekday: '#0969da',
                diaryMonth: '#0969da',
                diaryYear: '#0969da',
                financial: '#1a7f37',
                budgetRange: '#8250df'
            };
            
            const colorMap = this.currentTheme === 'light' ? lightColorMap : darkColorMap;
            
            const chartConfigs = [
                // Film panel charts (use panelDataSources.films)
                { id: 'countriesChart', data: filmSourceData.countries, color: colorMap.countries, category: 'countries' },
                { id: 'languagesChart', data: filmSourceData.languages, color: colorMap.languages, category: 'languages' },
                { id: 'genresChart', data: filmSourceData.genres, color: colorMap.genres, category: 'genres' },
                { id: 'decadesChart', data: filmSourceData.decades, color: colorMap.decades, category: 'decades' },
                // People panel charts (use panelDataSources.people)
                { id: 'directorsChart', data: peopleSourceData.directors, color: colorMap.directors, category: 'directors' },
                { id: 'actorsChart', data: peopleSourceData.actors, color: colorMap.actors, category: 'actors' },
                // Diary chart (always uses rawDiaryData - no toggle)
                { id: 'diaryChart', data: this.sortedDiaryData, color: colorMap.diaryWeekday, category: 'diaryWeekday' },
                // Finance panel charts (use panelDataSources.finance)
                { id: 'financialChart', data: financeSourceData[this.financialViewMode] || [], color: colorMap.financial, category: 'financial' },
                { id: 'budgetRangeChart', data: financeBudgetRangeSource, color: colorMap.budgetRange, category: 'budgetRange' }
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
        data.status = status;
        data.analysisProgress = status;
        data.filmsProcessed = filmsProcessed || 0;
        data.filmsTotal = filmsTotal || 0;
        data.processingSpeed = speed || 0;
        data.etaSeconds = etaSeconds || 0;
        
        // Parse step indicator from status (format: "Step X/Y: ...")
        const stepMatch = status.match(/^Step\s+(\d+)\/(\d+)/);
        if (stepMatch) {
            data.currentStep = parseInt(stepMatch[1]);
            data.totalSteps = parseInt(stepMatch[2]);
        } else {
            data.currentStep = 0;
            data.totalSteps = 0;
        }
    }
};
