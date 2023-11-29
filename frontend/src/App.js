import React from 'react'
import Navbar from './components/Navbar';
import Header from './components/Header';
import Menu from './components/Menu';
import Footer from './components/Footer';

const App = () => {
  return (
    <div className='relative'>
      <Navbar />
      <Header />
      <Menu />
      <Footer />
    </div>
  )
}

export default App;