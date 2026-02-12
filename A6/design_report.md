# Summary of patterns used, rationale, and tradeoffs

## Patterns Used

### 1. Singleton Pattern
**Purpose**: Single configuration instance across the system  
**Rationale**: Avoids duplicate config parsing, keeps state consistent  
**Tradeoffs**: Memory-efficient and simple, but harder to test and can be a concurrency bottleneck

### 2. Factory Pattern
**Purpose**: Centralize instrument creation (Stock, Bond, ETF)  
**Rationale**: Hides construction logic and simplifies extending types  
**Tradeoffs**: Easy to extend but adds an abstraction layer

### 3. Builder Pattern
**Purpose**: Construct portfolios with nested structures  
**Rationale**: Fluent API for complex portfolios from JSON  
**Tradeoffs**: Clear, composable, but more boilerplate

### 4. Composite Pattern
**Purpose**: Model portfolios as trees  
**Rationale**: Uniform handling of positions and sub-portfolios  
**Tradeoffs**: Natural hierarchies; harder to enforce type constraints at depth

### 5. Strategy Pattern
**Purpose**: Interchangeable trading strategies  
**Rationale**: Swap strategies without changing the engine  
**Tradeoffs**: Flexible but requires managing state per strategy

### 6. Observer Pattern
**Purpose**: Notify listeners when signals are generated  
**Rationale**: Decouples signal generation from handling  
**Tradeoffs**: Low coupling but unpredictable execution order

### 7. Command Pattern
**Purpose**: Encapsulate trade execution with undo/redo  
**Rationale**: Provides audit trail and undo capability  
**Tradeoffs**: Useful but increases complexity and memory usage

### 8. Adapter Pattern
**Purpose**: Standardize Yahoo/XML data formats  
**Rationale**: Isolate the system from external API changes  
**Tradeoffs**: Useful integration pattern with extra indirection

### 9. Decorator Pattern
**Purpose**: Add analytics without modifying instruments  
**Rationale**: Compose metrics (volatility, beta, drawdown) at runtime  
**Tradeoffs**: Flexible but more objects and harder debugging

---

## Pattern Interactions
- Factory → Decorator: Build instruments and add analytics
- Builder → Composite: Construct nested portfolios
- Strategy → Observer: Publish signals
- Strategy → Command: Execute trades from signals
- Adapter → Factory: Feed parsed data for creation

---

## Overall Tradeoffs
**Strengths**: Modular, testable, easy to extend  
**Costs**: More objects, higher complexity, learning curve