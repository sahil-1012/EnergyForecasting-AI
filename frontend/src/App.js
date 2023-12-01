import React from 'react'
import Navbar from './components/Navbar';
import Header from './components/Header';
import Menu from './components/Menu';
import Footer from './components/Footer';
import CarbonEmission from './components/CarbonEmission';

const App = () => {
  return (
    <div className='relative'>
      <Navbar />
      <Header />
      <Menu />
      <CarbonEmission />
      <Footer />
    </div>
  )
}

export default App;