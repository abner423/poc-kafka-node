import landingPage from './assets/landingPage.gif';
import './App.css';
import { Link } from "react-router-dom";

function App() {
  return (
    <div className="App">
      <header className="App-header">
        <div className='App-div-row'>
          <span className='App-div-intro'>
            <h2 className='App-div-animation typing-animation'>Construa novas conexões</h2>
            Aqui você pode conhecer mais pessoas através de uma interação única
            proporcionando amizades jamais esperadas, dos mais diversos lugares do planeta! Faça seu cadastro, e desfrute
            desse mundo virtual!
          </span>
          <img src={landingPage} alt="gif" />
        </div>

        <Link to="/signup">
          <button className="App-button"> Sign up</button>
        </Link>
      </header>
    </div>
  );
}

export default App;
