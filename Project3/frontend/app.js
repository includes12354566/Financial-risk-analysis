// 金融风险分析系统前端应用
class RiskAnalysisApp {
    constructor() {
        this.apiBaseUrl = 'http://localhost:8080';
        this.init();
    }

    init() {
        this.bindEvents();
        this.loadSystemStats();
    }

    bindEvents() {
        // 表单提交事件
        document.getElementById('queryForm').addEventListener('submit', (e) => {
            e.preventDefault();
            this.performRiskAnalysis();
        });

        // 指标条件变化事件
        ['metricA', 'metricB', 'metricC'].forEach(id => {
            document.getElementById(id).addEventListener('change', () => {
                this.updateMetricInputs();
            });
        });
    }

    updateMetricInputs() {
        const metricA = document.getElementById('metricA').checked;
        const metricB = document.getElementById('metricB').checked;
        const metricC = document.getElementById('metricC').checked;

        // 根据选择的指标更新输入框状态
        document.getElementById('minMetricA').disabled = !metricA;
        document.getElementById('minMetricB').disabled = !metricB;
        document.getElementById('maxMetricC').disabled = !metricC;

        // 设置默认值
        if (metricA) {
            document.getElementById('minMetricA').value = 1;
        }
        if (metricB) {
            document.getElementById('minMetricB').value = 1;
        }
        if (metricC) {
            document.getElementById('maxMetricC').value = 0;
        }
    }

    async loadSystemStats() {
        try {
            const response = await fetch(`${this.apiBaseUrl}/api/stats`);
            if (!response.ok) {
                throw new Error(`HTTP ${response.status}: ${response.statusText}`);
            }
            
            const stats = await response.json();
            this.displaySystemStats(stats);
        } catch (error) {
            console.error('加载系统统计失败:', error);
            document.getElementById('systemStats').innerHTML = `
                <div class="text-center text-danger">
                    <i class="fas fa-exclamation-triangle"></i>
                    <p class="mt-2">加载统计信息失败</p>
                </div>
            `;
        }
    }

    displaySystemStats(stats) {
        const s = {
            total_accounts: Number((stats && (
                stats.total_accounts ?? (stats.data && stats.data.total_accounts) ?? stats.accounts ?? (stats.data && stats.data.accounts)
            )) ?? 0),
            total_logins: Number((stats && (
                stats.total_logins ?? (stats.data && stats.data.total_logins) ?? stats.logins ?? (stats.data && stats.data.logins)
            )) ?? 0),
            total_transactions: Number((stats && (
                stats.total_transactions ?? (stats.data && stats.data.total_transactions) ?? stats.transactions ?? (stats.data && stats.data.transactions)
            )) ?? 0),
            large_transactions: Number((stats && (
                stats.large_transactions ?? (stats.data && stats.data.large_transactions) ?? stats.large ?? (stats.data && stats.data.large)
            )) ?? 0),
            timestamp: (stats && (
                stats.timestamp ?? (stats.data && stats.data.timestamp)
            )) || new Date().toISOString()
        };

        const statsHtml = `
            <div class="row g-2">
                <div class="col-6">
                    <div class="text-center">
                        <div class="h5 mb-0">${this.formatNumber(s.total_accounts)}</div>
                        <small class="text-muted">总账户数</small>
                    </div>
                </div>
                <div class="col-6">
                    <div class="text-center">
                        <div class="h5 mb-0">${this.formatNumber(s.total_logins)}</div>
                        <small class="text-muted">登录记录</small>
                    </div>
                </div>
                <div class="col-6">
                    <div class="text-center">
                        <div class="h5 mb-0">${this.formatNumber(s.total_transactions)}</div>
                        <small class="text-muted">转账记录</small>
                    </div>
                </div>
                <div class="col-6">
                    <div class="text-center">
                        <div class="h5 mb-0">${this.formatNumber(s.large_transactions)}</div>
                        <small class="text-muted">大额交易</small>
                    </div>
                </div>
            </div>
            <hr>
            <small class="text-muted">
                <i class="fas fa-clock me-1"></i>
                更新时间: ${new Date(s.timestamp).toLocaleString()}
            </small>
        `;
        document.getElementById('systemStats').innerHTML = statsHtml;
    }

    async performRiskAnalysis() {
        // 显示加载状态
        this.showLoading();
        // 清空旧结果，避免重复渲染叠加
        const list = document.getElementById('transactionsList');
        if (list) list.innerHTML = '';

        try {
            // 获取表单数据
            const formData = this.getFormData();
            
            // 发送API请求
            const response = await fetch(`${this.apiBaseUrl}/api/risk-analysis`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify(formData)
            });

            if (!response.ok) {
                throw new Error(`HTTP ${response.status}: ${response.statusText}`);
            }

            const result = await response.json();
            
            if (result.status === 'success') {
                this.displayResults(result);
            } else {
                throw new Error(result.error || '查询失败');
            }

        } catch (error) {
            console.error('风险分析失败:', error);
            this.showError(error.message);
        } finally {
            this.hideLoading();
        }
    }

    getFormData() {
        const metricA = document.getElementById('metricA').checked;
        const metricB = document.getElementById('metricB').checked;
        const metricC = document.getElementById('metricC').checked;

        return {
            time_range: document.getElementById('timeRange').value,
            min_metric_a: metricA ? parseInt(document.getElementById('minMetricA').value) : 0,
            min_metric_b: metricB ? parseInt(document.getElementById('minMetricB').value) : 0,
            max_metric_c: metricC ? parseFloat(document.getElementById('maxMetricC').value) : 999999999
        };
    }

    displayResults(result) {
        const transactions = result.transactions;
        
        if (transactions.length === 0) {
            this.showEmptyState();
            return;
        }

        // 显示查询信息
        this.displayQueryInfo(result);
        
        // 显示交易列表
        this.displayTransactions(transactions);
        
        // 显示结果区域
        document.getElementById('queryResults').style.display = 'block';
        document.getElementById('emptyState').style.display = 'none';
        document.getElementById('errorState').style.display = 'none';
    }

    displayQueryInfo(result) {
        const cDisabled = Number(result?.criteria?.max_metric_c ?? 0) >= 999999000;
        const queryInfoHtml = `
            <div class="row">
                <div class="col-md-6">
                    <div class="card result-card">
                        <div class="card-body">
                            <h6 class="card-title">
                                <i class="fas fa-info-circle me-2"></i>
                                查询信息
                            </h6>
                            <p class="mb-1"><strong>时间范围:</strong> ${this.getTimeRangeText(result.time_range)}</p>
                            <p class="mb-1"><strong>查询耗时:</strong> ${result.query_time_ms}ms</p>
                            <p class="mb-0"><strong>结果数量:</strong> ${result.total_count} 条</p>
                        </div>
                    </div>
                </div>
                <div class="col-md-6">
                    <div class="card result-card">
                        <div class="card-body">
                            <h6 class="card-title">
                                <i class="fas fa-filter me-2"></i>
                                筛选条件
                            </h6>
                            <p class="mb-1"><strong>指标A ≥</strong> ${result.criteria.min_metric_a}</p>
                            <p class="mb-1"><strong>指标B ≥</strong> ${result.criteria.min_metric_b}</p>
                            <p class="mb-0"><strong>指标C:</strong> ${cDisabled ? '未启用' : `≤ ${result.criteria.max_metric_c}`}</p>
                        </div>
                    </div>
                </div>
            </div>
        `;
        document.getElementById('queryInfo').innerHTML = queryInfoHtml;
        document.getElementById('resultCount').textContent = `${result.total_count} 条记录`;
    }

    displayTransactions(transactions) {
        // 去重防御：按 transaction_id 去重
        const seen = new Set();
        const unique = [];
        for (const tx of transactions) {
            if (!seen.has(tx.transaction_id)) {
                seen.add(tx.transaction_id);
                unique.push(tx);
            }
        }
        const transactionsHtml = unique.map(transaction => `
            <div class="card mb-3">
                <div class="card-header d-flex justify-content-between align-items-center">
                    <div>
                        <h6 class="mb-0">
                            <i class="fas fa-exchange-alt me-2"></i>
                            交易 #${transaction.transaction_id}
                        </h6>
                        <small class="text-muted">${transaction.transaction_time}</small>
                    </div>
                    <div>
                        <span class="badge bg-${this.getRiskLevelColor(transaction.risk_level)} fs-6">
                            ${this.getRiskLevelText(transaction.risk_level)}
                        </span>
                    </div>
                </div>
                <div class="card-body">
                    <div class="row">
                        <!-- 交易信息 -->
                        <div class="col-md-4">
                            <h6 class="text-primary">
                                <i class="fas fa-money-bill-wave me-2"></i>
                                交易信息
                            </h6>
                            <p class="mb-1"><strong>金额:</strong> ¥${this.formatCurrency(transaction.amount)}</p>
                            <p class="mb-0"><strong>描述:</strong> ${transaction.description || '无'}</p>
                        </div>
                        
                        <!-- 受害者账户 -->
                        <div class="col-md-4">
                            <h6 class="text-danger">
                                <i class="fas fa-user-times me-2"></i>
                                受害者账户
                            </h6>
                            <p class="mb-1"><strong>账户ID:</strong> ${transaction.victim_account.account_id}</p>
                            <p class="mb-1"><strong>姓名:</strong> ${transaction.victim_account.name}</p>
                            <p class="mb-1"><strong>电话:</strong> ${transaction.victim_account.phone}</p>
                            <p class="mb-0"><strong>类型:</strong> ${this.getAccountTypeText(transaction.victim_account.type)}</p>
                        </div>
                        
                        <!-- 可疑账户 -->
                        <div class="col-md-4">
                            <h6 class="text-warning">
                                <i class="fas fa-user-shield me-2"></i>
                                可疑账户
                            </h6>
                            <p class="mb-1"><strong>账户ID:</strong> ${transaction.suspicious_account.account_id}</p>
                            <p class="mb-1"><strong>姓名:</strong> ${transaction.suspicious_account.name}</p>
                            <p class="mb-1"><strong>电话:</strong> ${transaction.suspicious_account.phone}</p>
                            <p class="mb-0"><strong>类型:</strong> ${this.getAccountTypeText(transaction.suspicious_account.type)}</p>
                        </div>
                    </div>
                    
                    <!-- 风险指标 -->
                    <hr>
                    <div class="row">
                        <div class="col-12">
                            <h6 class="mb-3">
                                <i class="fas fa-chart-line me-2"></i>
                                风险指标
                            </h6>
                            <div class="d-flex gap-3 flex-wrap">
                                <span class="metric-badge metric-a">
                                    <i class="fas fa-arrow-down me-1"></i>
                                    指标A: ${transaction.risk_metrics.metric_a}
                                </span>
                                <span class="metric-badge metric-b">
                                    <i class="fas fa-sign-in-alt me-1"></i>
                                    指标B: ${transaction.risk_metrics.metric_b}
                                </span>
                                <span class="metric-badge metric-c">
                                    <i class="fas fa-coins me-1"></i>
                                    指标C: ¥${this.formatCurrency(transaction.risk_metrics.metric_c)}
                                </span>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        `).join('');

        document.getElementById('transactionsList').innerHTML = transactionsHtml;
    }

    showLoading() {
        document.getElementById('loadingIndicator').classList.add('show');
        document.getElementById('queryResults').style.display = 'none';
        document.getElementById('emptyState').style.display = 'none';
        document.getElementById('errorState').style.display = 'none';
    }

    hideLoading() {
        document.getElementById('loadingIndicator').classList.remove('show');
    }

    showEmptyState() {
        document.getElementById('queryResults').style.display = 'none';
        document.getElementById('emptyState').style.display = 'block';
        document.getElementById('errorState').style.display = 'none';
    }

    showError(message) {
        document.getElementById('errorMessage').textContent = message;
        document.getElementById('queryResults').style.display = 'none';
        document.getElementById('emptyState').style.display = 'none';
        document.getElementById('errorState').style.display = 'block';
    }

    // 工具函数
    formatNumber(num) {
        return new Intl.NumberFormat('zh-CN').format(num);
    }

    formatCurrency(amount) {
        return new Intl.NumberFormat('zh-CN', {
            style: 'currency',
            currency: 'CNY',
            minimumFractionDigits: 2
        }).format(amount);
    }

    getTimeRangeText(timeRange) {
        const ranges = {
            '24h': '24小时',
            '3d': '3天',
            '7d': '7天',
            '30d': '30天',
            '6m': '6个月',
            '1y': '1年'
        };
        return ranges[timeRange] || timeRange;
    }

    getRiskLevelColor(level) {
        const colors = {
            'HIGH': 'danger',
            'MEDIUM': 'warning',
            'LOW': 'success'
        };
        return colors[level] || 'secondary';
    }

    getRiskLevelText(level) {
        const texts = {
            'HIGH': '高风险',
            'MEDIUM': '中风险',
            'LOW': '低风险'
        };
        return texts[level] || level;
    }

    getAccountTypeText(type) {
        const types = {
            'personal': '个人账户',
            'business': '企业账户'
        };
        return types[type] || type;
    }
}

// 初始化应用
document.addEventListener('DOMContentLoaded', () => {
    new RiskAnalysisApp();
});











