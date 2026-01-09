import { Routes as RouterRoutes, Route, Navigate } from "react-router-dom";
import { Layout } from "./Layout";

import { DashboardPage } from "../pages/Dashboard/DashboardPage";
import { DataPage } from "../pages/Data/DataPage";
import { BacktestingPage } from "../pages/Backtesting/BacktestingPage";
import { MLModelsPage } from "../pages/MLModels/MLModelsPage";
import { PortfolioPage } from "../pages/Portfolio/PortfolioPage";
import { StrategiesPage } from "../pages/Strategies/StrategiesPage";

export function Routes() {
  return (
    <RouterRoutes>
      <Route element={<Layout />}>
        <Route path="/" element={<DashboardPage />} />
        <Route path="/data/*" element={<DataPage />} />
        <Route path="/backtesting" element={<BacktestingPage />} />
        <Route path="/ml-models" element={<MLModelsPage />} />
        <Route path="/portfolio" element={<PortfolioPage />} />
        <Route path="/strategies" element={<StrategiesPage />} />
      </Route>

      {/* Hard fallback */}
      <Route path="*" element={<Navigate to="/" replace />} />
    </RouterRoutes>
  );
}
