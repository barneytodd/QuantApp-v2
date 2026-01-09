import { Routes, Route } from "react-router-dom";
import { Layout } from "./Layout";
import { DashboardPage } from "../pages/Dashboard/DashboardPage";
import { DataPage } from "../pages/Data";
import { IngestionTab, ValidationTab, DataVisualisationTab } from "../pages/Data";
import { BacktestingPage } from "../pages/Backtesting/BacktestingPage";
import { MLModelsPage } from "../pages/MLModels/MLModelsPage";
import { PortfolioPage } from "../pages/Portfolio/PortfolioPage";
import { StrategiesPage } from "../pages/Strategies/StrategiesPage";

export function AppRoutes() {
  return (
    <Routes>
      <Route element={<Layout />}>
        <Route path="/" element={<DashboardPage />} />
        <Route path="/data" element={<DataPage />}>
          <Route path="ingestion" element={<IngestionTab />} />
          <Route path="validation" element={<ValidationTab />} />
          <Route path="view" element={<DataVisualisationTab />} />
          <Route index element={<IngestionTab />} /> {/* default tab */}
        </Route>
        <Route path="/backtesting" element={<BacktestingPage />} />
        <Route path="/ml-models" element={<MLModelsPage />} />
        <Route path="/portfolio" element={<PortfolioPage />} />
        <Route path="/strategies" element={<StrategiesPage />} />
      </Route>
    </Routes>
  );
}
