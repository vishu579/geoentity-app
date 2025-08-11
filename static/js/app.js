new Vue({
    el: '#app',
    delimiters: ["${", "}"],
    data: {
        rawData: [],
        filteredData: [],
        searchQuery: '',
        loading: false,
        error: null,
        selectedSortKey: '',
        sortAsc: true,
        sortableKeys: ['id', 'name', 'category', 'parent_id', 'project', 'theme'],
        selectedCategory: '',
        selectedTheme: ''
    },
    computed: {
        keyLabels() {
            return {
                id: 'ID',
                name: 'Name',
                category: 'Category',
                parent_id: 'Parent ID',
                project: 'Project',
                theme: 'Theme'
            };
        },
        sortableKeysWithoutCategoryTheme() {
            return this.sortableKeys.filter(k => k !== 'category' && k !== 'theme');
        },
        uniqueCategories() {
            const cats = this.rawData
                .map(item => item.category)
                .filter(c => c != null && c !== '');
            return [...new Set(cats)].sort();
        },
        uniqueThemes() {
            const ths = this.rawData
                .map(item => item.theme)
                .filter(t => t != null && t !== '');
            return [...new Set(ths)].sort();
        }
    },
    methods: {
        fetchData() {
            this.loading = true;
            this.error = null;
            fetch('https://vedas.sac.gov.in/geoentity-services/api/geoentity-sources/')
                .then(res => res.json())
                .then(data => {
                    this.rawData = data.data;
                    this.filteredData = this.rawData.slice();
                })
                .catch(() => {
                    this.error = 'Failed to load data';
                })
                .finally(() => {
                    this.loading = false;
                });
        },
        filterData() {
            const q = this.searchQuery.toLowerCase().trim();
            this.filteredData = this.rawData.filter(item => {
                const matchesSearch =
                    !q ||
                    (item.name && item.name.toLowerCase().includes(q)) ||
                    (item.category && item.category.toLowerCase().includes(q)) ||
                    (item.project && item.project.toLowerCase().includes(q)) ||
                    (item.theme && item.theme.toLowerCase().includes(q)) ||
                    (item.id && item.id.toString().includes(q)) ||
                    (item.parent_id !== null && item.parent_id.toString().includes(q));

                const matchesCategory = !this.selectedCategory || item.category === this.selectedCategory;
                const matchesTheme = !this.selectedTheme || item.theme === this.selectedTheme;

                return matchesSearch && matchesCategory && matchesTheme;
            });

            if (this.selectedSortKey) {
                this.sortBy(this.selectedSortKey, false);
            }
        },
        sortBy(key, toggle = true) {
            if (toggle) {
                if (this.selectedSortKey === key) {
                    this.sortAsc = !this.sortAsc;
                } else {
                    this.selectedSortKey = key;
                    this.sortAsc = true;
                }
            }

            this.filteredData.sort((a, b) => {
                const valA = a[key] != null ? a[key] : '';
                const valB = b[key] != null ? b[key] : '';
                if (typeof valA === 'number' && typeof valB === 'number') {
                    return this.sortAsc ? valA - valB : valB - valA;
                }
                return this.sortAsc
                    ? String(valA).localeCompare(String(valB))
                    : String(valB).localeCompare(String(valA));
            });
        },
        handleSortChange() {
            if (this.selectedSortKey) {
                this.sortBy(this.selectedSortKey, false);
            }
        },
        toggleSortOrder() {
            this.sortAsc = !this.sortAsc;
            if (this.selectedSortKey) {
                this.sortBy(this.selectedSortKey, false);
            }
        }
    },
    mounted() {
        this.fetchData();

        // Only initialize Flatpickr if library is loaded and element exists
        const dateInput = document.querySelector("#publish_date_yyyymmdd");
        if (typeof flatpickr !== "undefined" && dateInput) {
            flatpickr(dateInput, {
                dateFormat: "Ymd", // Visible format: YYYYMMDD
                onChange: (selectedDates, dateStr) => {
                    this.publishDate = dateStr; // Update Vue model
                }
            });
        }
    }
});  