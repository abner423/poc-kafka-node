import App from "./App";
import { BrowserRouter, Route, Routes } from 'react-router-dom';
import Signup from "./Signup";

function routes() {
    return (
        <BrowserRouter>
            <Routes>
                <Route path="/" exact element={<App />} />
                <Route path="/signup" exact element={<Signup />} />
            </Routes>
        </BrowserRouter>
    );
}

export default routes;