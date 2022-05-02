import "./Signup.css";
import { useNavigate } from "react-router-dom";
import Check from './assets/check.png';
import Head from './assets/head.png';
import Dotted from './assets/dotted.jpg';
import { useState } from "react";

import api from "./api";

function Signup() {
  let navigate = useNavigate();
  const [nome, setNome] = useState("");
  const [email, setEmail] = useState("");
  const [resumo, setResumo] = useState("");

  async function clique() {

    if (nome.trim() !== "" && email.trim() !== "" && resumo.trim() !== "") {
      console.log(`Nome: ${nome} / Email: ${email} / Resumo: ${resumo}`);
      const data = {
        nome,
        email,
        resumo
      };
      const response = await api.post("users", data);
      navigate("/", { replace: true });
      console.log(response);
    } else {

    }
  }

  return (
    <div className="signup">
      <div className="blue"></div>
      <div className="dotted">
        <img src={Dotted} alt="pontilhado" />
      </div>
      <div className="box">
        <div className="box_left">
          <div className="header">
            <h1>Cadastre-se</h1>
            <p>
              Ao se cadastrar você concorda com nossos termos de serviço e
              política de privacidade.
            </p>
          </div>

          <div className="input">
            <input placeholder="nome" value={nome} onChange={(e) => setNome(e.target.value)}></input>
          </div>
          <div className="input">
            <input placeholder="nome@empresa.com" value={email} onChange={(e) => setEmail(e.target.value)}></input>
          </div>
          <div className="input">
            <textarea cols={33} rows={10} placeholder="resumo sobre usuário" value={resumo} onChange={(e) => setResumo(e.target.value)}></textarea>
          </div>
          <button
            onClick={clique}
          >cadastrar</button>
        </div>
        <div className="box_right">
          <img className="head" src={Head} alt='head' />
          <h1>Benefícios</h1>

          <div className="benefits">
            <div>
              <img src={Check} alt='check' />
              <p>Construir novas conexões</p>
            </div>
            <div>
              <img src={Check} alt='check' />
              <p>Compartilhe seus momentos</p>
            </div>
            <div>
              <img src={Check} alt='check' />
              <p>Explore novas fronteiras</p>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

export default Signup;
