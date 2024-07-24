import { createBrowserRouter } from 'react-router-dom';
import Transactions from './routes/Transactions';
import Transaction from './routes/Transaction';
import Home from './routes/Home';
import Account from './routes/Account';
import Accounts from './routes/Accounts';
import AccountOverview from './routes/Account/Overview';
import AccountTransactions from './routes/Account/UserTransactions';
import AccountResources from './routes/Account/Resources';
import AccountModules from './routes/Account/Modules';
import Block from './routes/Block';
import Root from './Root';
import Validators from './routes/Validators';
import CommunityWallets from './routes/CommunityWallets';
import Module from './routes/Account/Modules/Module';
import Stats from './routes/Stats';
import Postero from './routes/Postero';

const router = createBrowserRouter([
  {
    path: '/',
    element: <Root />,
    children: [
      {
        path: '/',
        element: <Home />,
      },
      {
        path: '/accounts/:accountAddress',
        element: <Account />,
        children: [
          {
            index: true,
            element: <AccountOverview />,
          },
          {
            path: 'transactions',
            element: <AccountTransactions />,
          },
          {
            path: 'resources',
            element: <AccountResources />,
          },
          {
            path: 'modules',
            element: <AccountModules />,
            children: [
              {
                path: ':moduleName',
                element: <Module />,
              },
            ],
          },
        ],
      },
      {
        path: '/transactions',
        element: <Transactions />,
      },
      {
        path: '/transactions/:version',
        element: <Transaction />,
      },
      {
        path: '/blocks/:blockHeight',
        element: <Block />,
      },
      {
        path: '/accounts',
        element: <Accounts />,
      },
      {
        path: '/validators',
        element: <Validators />,
      },
      {
        path: '/community-wallets',
        element: <CommunityWallets />,
      },
      {
        path: '/stats',
        element: <Stats />,
      },
      {
        path: '/postero',
        element: <Postero />,
      },
    ],
  },
]);

export default router;
