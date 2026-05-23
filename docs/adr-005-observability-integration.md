# ADR-005: Observability Integration

## Context
Проект использовал stdlib `logging` в adapter/worker/ и не имел трейсов/метрик в usecase/ и repository/

## Decision
- Logger, Tracer, Metrics прокидываются через DI (конструкторы) на всех слоях
- Протоколы определены в `usecase/interface.py`, реализации в `adapter/observability/`
- Ошибки логируются только в точке перехвата (adapter/worker/)
- debug/info/warning используются свободно на любом слое
- Spans оборачивают значимые I/O операции на всех слоях
- Метрики (counters/histograms) на бизнес-события в usecase/ и adapter/

## Consequences
- Единая точка логирования ошибок упрощает отладку
- Структурированные attrs вместо format-строк
- Все зависимости observability явные, тестируемые через Noop-реализации
