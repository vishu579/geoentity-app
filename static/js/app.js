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
        sortableKeys: ['id', 'name', 'category', 'parent_id', 'project', 'theme']
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
            if (!q) {
                this.filteredData = this.rawData.slice();
            } else {
                this.filteredData = this.rawData.filter(item => {
                    return (
                        (item.name && item.name.toLowerCase().includes(q)) ||
                        (item.category && item.category.toLowerCase().includes(q)) ||
                        (item.project && item.project.toLowerCase().includes(q)) ||
                        (item.theme && item.theme.toLowerCase().includes(q)) ||
                        (item.id && item.id.toString().includes(q)) ||
                        (item.parent_id !== null && item.parent_id.toString().includes(q))
                    );
                });
            }

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
    }
});  